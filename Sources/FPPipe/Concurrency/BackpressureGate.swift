import Synchronization

/// Bounds how far a single producer task may run ahead of its consumer.
///
/// The producer calls `acquire()` before yielding each element; the consumer calls
/// `release()` as it takes each element off the buffer. Once `capacity` elements are
/// in flight the producer suspends until the consumer catches up — this is what gives
/// the fan-out stages (`FlatMapSequence`, `FlatMapAsyncSequence`) bounded memory while
/// keeping their eager-producer model.
///
/// `acquire()` is cancellation-aware: when the producer's task is cancelled it returns
/// immediately (without taking a slot) so the producer can observe `Task.isCancelled`
/// and tear down — a parked expansion never outlives an abandoned consumer.
///
/// Supports exactly one producer: at most one `acquire()` may be suspended at a time.
final class BackpressureGate: Sendable {
    private struct State {
        var available: Int
        var waiter: CheckedContinuation<Void, Never>?
        var cancelled = false
    }

    private let mutex: Mutex<State>

    init(capacity: Int) {
        self.mutex = Mutex(State(available: max(1, capacity)))
    }

    /// Producer side: returns when a buffer slot is free, or immediately on task
    /// cancellation (no slot is taken — the caller re-checks `Task.isCancelled`).
    func acquire() async {
        await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                let resumeNow = mutex.withLock { state -> Bool in
                    if state.cancelled { return true }
                    if state.available > 0 {
                        state.available -= 1
                        return true
                    }
                    state.waiter = continuation
                    return false
                }
                if resumeNow { continuation.resume() }
            }
        } onCancel: {
            let waiter = mutex.withLock { state -> CheckedContinuation<Void, Never>? in
                state.cancelled = true
                let waiter = state.waiter
                state.waiter = nil
                return waiter
            }
            waiter?.resume()
        }
    }

    /// Consumer side: one element was taken off the buffer; free its slot. If the
    /// producer is parked, the slot transfers to it directly.
    func release() {
        let waiter = mutex.withLock { state -> CheckedContinuation<Void, Never>? in
            if let waiter = state.waiter {
                state.waiter = nil
                return waiter
            }
            state.available += 1
            return nil
        }
        waiter?.resume()
    }
}

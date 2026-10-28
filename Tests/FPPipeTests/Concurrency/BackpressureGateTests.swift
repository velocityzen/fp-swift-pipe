@testable import FPPipe
import Synchronization
import Testing

/// Polls `condition` for up to ~1s; parked/unparked states are observed, not timed.
private func eventually(_ condition: () -> Bool) async -> Bool {
    for _ in 0..<100 {
        if condition() { return true }
        try? await Task.sleep(nanoseconds: 10_000_000)
    }
    return condition()
}

@Test
func gateHandsOutCapacitySlotsWithoutParking() async {
    let gate = BackpressureGate(capacity: 3)
    await gate.acquire()
    await gate.acquire()
    await gate.acquire()
    // Reaching here means none of the three acquires parked.
}

@Test
func gateParksAtCapacityUntilReleased() async {
    let gate = BackpressureGate(capacity: 1)
    await gate.acquire()

    let acquired = Mutex(false)
    let producer = Task {
        await gate.acquire()
        acquired.withLock { $0 = true }
    }

    try? await Task.sleep(nanoseconds: 50_000_000)
    #expect(acquired.withLock { $0 } == false, "acquire returned with no free slot")

    gate.release()
    await producer.value
    #expect(acquired.withLock { $0 })
}

@Test
func gateBanksReleasesMadeWhileNobodyWaits() async {
    let gate = BackpressureGate(capacity: 1)
    await gate.acquire()
    gate.release()
    gate.release()
    // Both released slots are banked: two more acquires return without parking.
    await gate.acquire()
    await gate.acquire()
}

@Test
func gateCancellationUnparksTheProducer() async {
    let gate = BackpressureGate(capacity: 1)
    await gate.acquire()

    let parked = Mutex(false)
    let producer = Task {
        parked.withLock { $0 = true }
        await gate.acquire()
        return Task.isCancelled
    }

    #expect(await eventually { parked.withLock { $0 } })
    try? await Task.sleep(nanoseconds: 20_000_000)
    producer.cancel()
    // No release: only cancellation can return the parked acquire.
    #expect(await producer.value)
}

@Test
func gateAcquireReturnsImmediatelyWhenAlreadyCancelled() async {
    let gate = BackpressureGate(capacity: 1)
    await gate.acquire()

    let producer = Task {
        while !Task.isCancelled { await Task.yield() }
        await gate.acquire()
        await gate.acquire()
        return true
    }
    producer.cancel()
    #expect(await producer.value)
}

// MARK: - Argument validation traps

#if os(macOS) || os(Linux)
@Test
func invalidConcurrencyTraps() async {
    await #expect(processExitsWith: .failure) {
        _ = AsyncMap(concurrency: 0) { (n: Int) async -> Int in n }
    }
}

@Test
func invalidBufferSizeTraps() async {
    await #expect(processExitsWith: .failure) {
        _ = FlatMapSequence(bufferSize: 0) { (n: Int) in 0..<n }
    }
}

@Test
func negativeTakeCountTraps() async {
    await #expect(processExitsWith: .failure) {
        _ = Take(-1)
    }
}
#endif

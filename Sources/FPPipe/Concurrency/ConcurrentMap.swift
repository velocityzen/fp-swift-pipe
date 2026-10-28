/// Internal helper for bounded-concurrency, unordered async mapping over an
/// `AsyncSequence`. Used by the unordered `Async*` stages when `concurrency > 1`.
/// (The `KeepOrder` stages delegate to fp-swift's `mapAsyncKeepOrder` /
/// `flatMapAsyncKeepOrder` instead.)
///
/// Backpressure: a slot is taken when an element is admitted and given back only when
/// the consumer takes its result (or the transform drops it), so at most `concurrency`
/// elements are being transformed or waiting to be consumed. A slow consumer pauses the
/// source instead of growing a buffer — the same model as fp-swift's `mapAsyncKeepOrder`.
///
/// Latency: the source is read by a producer loop that never waits on emission, and each
/// transform yields its result the moment it completes, so a slow source never holds back
/// results that are already done.
///
/// Passthrough: an element for which `passthrough` returns a value is emitted as that
/// value without spawning a child task — used for the channel a stage leaves untouched
/// (e.g. failures through `AsyncMap`). It still takes a slot, so the bound holds.
///
/// Cancellation: cancelling the consumer cancels the producer and every in-flight
/// transform. Cooperative transforms (those that await something cancellation-aware)
/// short-circuit; non-cooperative CPU-bound transforms still run to completion, but the
/// consumer never waits on them.
func compactMapAsyncUnordered<Source, T>(
    _ source: Source,
    concurrency: Int,
    passthrough: @escaping @Sendable (Source.Element) -> T? = { _ in nil },
    _ transform: @concurrent @escaping @Sendable (Source.Element) async -> T?,
) -> some AsyncSequence<T, Never> & Sendable
where
    Source: AsyncSequence & Sendable,
    Source.Element: Sendable,
    Source.Failure == Never,
    T: Sendable
{
    let gate = BackpressureGate(capacity: concurrency)
    // The gate bounds buffer occupancy to `concurrency`, so `.unbounded` only needs to
    // be lossless, never to bound.
    let stream = AsyncStream<T>(bufferingPolicy: .unbounded) { continuation in
        let task = Task {
            // Transforms are children of the group, so cancelling `task` cancels them,
            // and the group returning means every admitted transform has finished.
            await withTaskGroup(of: Void.self) { group in
                for await element in source {
                    await gate.acquire()
                    if Task.isCancelled { break }
                    if let value = passthrough(element) {
                        continuation.yield(value)
                        continue
                    }
                    group.addTask {
                        if let value = await transform(element) {
                            continuation.yield(value)
                        } else {
                            gate.release()
                        }
                    }
                }
            }
            continuation.finish()
        }
        continuation.onTermination = { _ in task.cancel() }
    }
    return stream.map { (value: T) -> T in
        gate.release()
        return value
    }
}

/// `compactMapAsyncUnordered` for transforms that never drop an element.
func mapAsyncUnordered<Source, T>(
    _ source: Source,
    concurrency: Int,
    passthrough: @escaping @Sendable (Source.Element) -> T? = { _ in nil },
    _ transform: @concurrent @escaping @Sendable (Source.Element) async -> T,
) -> some AsyncSequence<T, Never> & Sendable
where
    Source: AsyncSequence & Sendable,
    Source.Element: Sendable,
    Source.Failure == Never,
    T: Sendable
{
    compactMapAsyncUnordered(source, concurrency: concurrency, passthrough: passthrough) {
        (element: Source.Element) async -> T? in
        .some(await transform(element))
    }
}

extension Result {
    /// The failure re-typed under a new success type, or `nil` for a success — the
    /// `passthrough` for stages that only transform successes.
    func passthroughFailure<NewSuccess>() -> Result<NewSuccess, Failure>? {
        guard case .failure(let error) = self else { return nil }
        return .failure(error)
    }

    /// The success re-typed under a new failure type, or `nil` for a failure — the
    /// `passthrough` for stages that only transform failures.
    func passthroughSuccess<NewFailure>() -> Result<Success, NewFailure>? {
        guard case .success(let value) = self else { return nil }
        return .success(value)
    }
}

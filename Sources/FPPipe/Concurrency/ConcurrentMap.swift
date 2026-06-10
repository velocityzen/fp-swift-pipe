/// Internal helpers for bounded-concurrency, async element mapping over an
/// `AsyncSequence`. Used by the unordered `Async*` stages when `concurrency > 1`.
/// (The `KeepOrder` stages delegate to fp-swift's `mapAsyncKeepOrder` /
/// `flatMapAsyncKeepOrder` instead.)
///
/// - `mapAsyncUnordered`        — yields each result the moment its task finishes.
/// - `compactMapAsyncUnordered` — same, with `nil` results dropped (thin layer over the above).
///
/// Cancellation: each helper proactively cancels in-flight transforms when the outer Task
/// is cancelled. Cooperative transforms (those that await something cancellation-aware)
/// will short-circuit; non-cooperative CPU-bound transforms still run to completion, but
/// the consumer no longer waits past the currently-resolving result.

/// Wraps a draining `Task` in an `AsyncStream`, finishing the continuation when the body
/// returns and cancelling the task on stream termination. Eliminates the boilerplate that
/// every helper below would otherwise repeat.
private func asyncStream<T: Sendable>(
    _ body: @escaping @Sendable (AsyncStream<T>.Continuation) async -> Void,
) -> AsyncStream<T> {
    // `.unbounded` is explicit (not the default fallback) — `AsyncStream` doesn't natively
    // backpressure. In practice the body's loop yields one result at a time and only
    // proceeds after `await group.next()`, so the queue holds at most `concurrency` items.
    AsyncStream<T>(bufferingPolicy: .unbounded) { continuation in
        let task = Task {
            await body(continuation)
            continuation.finish()
        }
        continuation.onTermination = { _ in
            task.cancel()
        }
    }
}

func mapAsyncUnordered<Source, T>(
    _ source: Source,
    concurrency: Int,
    _ transform: @escaping @Sendable (Source.Element) async -> T,
) -> AsyncStream<T>
where
    Source: AsyncSequence & Sendable,
    Source.Element: Sendable,
    Source.Failure == Never,
    T: Sendable
{
    asyncStream { continuation in
        await withTaskGroup(of: T.self) { group in
            var iter = source.makeAsyncIterator()

            // Prime up to N tasks.
            for _ in 0..<concurrency {
                if Task.isCancelled { break }
                guard let element = try? await iter.next() else {
                    break
                }

                group.addTask {
                    await transform(element)
                }
            }

            // Drain: every completion emits a result and pulls the next source element.
            while let result = await group.next() {
                if Task.isCancelled {
                    group.cancelAll()
                    break
                }
                continuation.yield(result)
                if let element = try? await iter.next() {
                    group.addTask {
                        await transform(element)
                    }
                }
            }
        }
    }
}

func compactMapAsyncUnordered<Source, T>(
    _ source: Source,
    concurrency: Int,
    _ transform: @escaping @Sendable (Source.Element) async -> T?,
) -> AsyncStream<T>
where
    Source: AsyncSequence & Sendable,
    Source.Element: Sendable,
    Source.Failure == Never,
    T: Sendable
{
    asyncStream { continuation in
        for await result in mapAsyncUnordered(source, concurrency: concurrency, transform) {
            if Task.isCancelled { break }
            if let value = result {
                continuation.yield(value)
            }
        }
    }
}

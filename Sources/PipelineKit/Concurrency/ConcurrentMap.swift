/// Internal helpers for bounded-concurrency, async element mapping over an
/// `AsyncSequence`. Used by every `Async*` stage when `concurrency > 1`.
///
/// - `mapAsyncUnordered`        — yields each result the moment its task finishes.
/// - `compactMapAsyncUnordered` — same, with `nil` results dropped (thin layer over the above).
/// - `mapAsyncKeepOrderBounded` — yields in strict source order, draining the head
///                                of a sliding window of pending tasks.
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
    AsyncStream<T> { continuation in
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

func mapAsyncKeepOrderBounded<Source, T>(
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
        var iter = source.makeAsyncIterator()
        var window: [Task<T, Never>] = []

        // Prime the sliding window with up to N tasks.
        for _ in 0..<concurrency {
            if Task.isCancelled { break }
            guard let element = try? await iter.next() else {
                break
            }
            window.append(
                Task { await transform(element) }
            )
        }

        // Drain head, refill back, preserving source order. Cancellation is checked both
        // before awaiting the head (so a non-cooperative head doesn't strand pending work)
        // and after, before refilling.
        while !window.isEmpty {
            if Task.isCancelled {
                for pending in window { pending.cancel() }
                window.removeAll()
                break
            }

            let head = window.removeFirst()
            continuation.yield(await head.value)

            if Task.isCancelled {
                for pending in window { pending.cancel() }
                window.removeAll()
                break
            }

            if let element = try? await iter.next() {
                window.append(
                    Task { await transform(element) }
                )
            }
        }
    }
}

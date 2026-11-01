import FP

/// Concurrently transforms successes via an async function while **preserving source order**.
///
/// `concurrency` caps the number of in-flight transforms. With the default of 1 this is
/// strictly sequential. With `concurrency > 1` up to N closures run in parallel and results
/// emit in the original source order — slow elements hold back faster downstream ones.
@usableFromInline
struct AsyncMapKeepOrderStage<Input: Sendable, Output: Sendable>: PipePolyStage {
    @usableFromInline let transform: @Sendable (Input) async -> Output
    @usableFromInline let concurrency: Int

    @inlinable
    init(_ transform: @escaping @Sendable (Input) async -> Output, concurrency: Int) {
        self.transform = transform
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let transform = self.transform
        if concurrency == 1 {
            let step = AsyncMapStep<Input, Output, F>(transform)
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        return .iterating {
            .init(
                upstream.mapAsyncKeepOrder(concurrency: concurrency, transform)
                    .makeAsyncIterator(),
            )
        }
    }
}

/// DSL: `AsyncMapKeepOrder { url in await fetch(url) }` (sequential) or
/// `AsyncMapKeepOrder(concurrency: 10) { url in await fetch(url) }` (10 in flight, ordered emit).
@inlinable
public func AsyncMapKeepOrder<Input: Sendable, Output: Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Output,
) -> some PipePolyStage<Input, Output> {
    AsyncMapKeepOrderStage(transform, concurrency: concurrency)
}

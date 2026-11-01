import FP

/// Asynchronously maps the success channel via a non-throwing async function.
/// Failures pass through unchanged.
///
/// `concurrency` controls how many transforms run in parallel. With the default of 1
/// the closure runs strictly sequentially. With `concurrency > 1` up to N closures run
/// in flight at once and **results emit as they complete (unordered)** — for source-
/// order-preserving parallelism use `AsyncMapKeepOrder`.
@usableFromInline
struct AsyncMapStage<Input: Sendable, Output: Sendable>: PipePolyStage {
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
        let step = AsyncMapStep<Input, Output, F>(transform)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        return .iterating {
            .init(
                upstream.compactMapAsyncUnordered(
                    concurrency: concurrency,
                    passthrough: { $0.passthroughFailure() },
                    { element in await step(element) },
                )
                .makeAsyncIterator(),
            )
        }
    }
}

@usableFromInline
struct AsyncMapStep<Input, Output, F: Error>: AsyncPipeStep {
    @usableFromInline let transform: @Sendable (Input) async -> Output

    @inlinable
    init(_ transform: @escaping @Sendable (Input) async -> Output) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(_ element: Result<Input, F>) async -> Result<Output, F>? {
        await element.mapAsync(transform)
    }
}

/// DSL: `AsyncMap { value in await transform(value) }` (sequential) or
/// `AsyncMap(concurrency: 10) { value in await transform(value) }` (10 in flight, unordered emit).
@inlinable
public func AsyncMap<Input: Sendable, Output: Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Output,
) -> some PipePolyStage<Input, Output> {
    AsyncMapStage(transform, concurrency: concurrency)
}

import FP

/// Asynchronously maps the success channel via a `Result`-returning async function.
/// A `.failure` returned by the transform short-circuits that element.
///
/// `concurrency` controls how many transforms run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` up to N closures run in flight and results emit
/// as they complete (unordered).
@usableFromInline
struct AsyncFlatMapStage<Input: Sendable, Output: Sendable, Failure: Error & Sendable>:
    PipeStage
{
    @usableFromInline let transform: @Sendable (Input) async -> Result<Output, Failure>
    @usableFromInline let concurrency: Int

    @inlinable
    init(
        _ transform: @escaping @Sendable (Input) async -> Result<Output, Failure>,
        concurrency: Int,
    ) {
        self.transform = transform
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    @inlinable
    func attach(_ upstream: Pipe<Input, Failure>) -> Pipe<Output, Failure> {
        let step = AsyncFlatMapStep(transform)
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
struct AsyncFlatMapStep<Input, Output, Failure: Error>: AsyncPipeStep {
    @usableFromInline let transform: @Sendable (Input) async -> Result<Output, Failure>

    @inlinable
    init(_ transform: @escaping @Sendable (Input) async -> Result<Output, Failure>) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(_ element: Result<Input, Failure>) async -> Result<Output, Failure>? {
        await element.flatMapAsync(transform)
    }
}

/// DSL: `AsyncFlatMap { value in await fetch(value) }` or
/// `AsyncFlatMap(concurrency: 10) { value in await fetch(value) }`.
@inlinable
public func AsyncFlatMap<Input: Sendable, Output: Sendable, Failure: Error & Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Result<Output, Failure>,
) -> some PipeStage<Input, Output, Failure> {
    AsyncFlatMapStage(transform, concurrency: concurrency)
}

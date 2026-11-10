import FP

/// The stage that `AsyncFlatMap` returns.
@usableFromInline
struct AsyncFlatMapStage<Input: Sendable, Output: Sendable, Failure: Error & Sendable>:
    PipeStage
{
    @usableFromInline let transform: @Sendable (Input) async -> Result<Output, Failure>
    @usableFromInline let concurrency: Int
    @usableFromInline let ordered: Bool
    @usableFromInline let bufferLimit: Int

    @inlinable
    init(
        _ transform: @escaping @Sendable (Input) async -> Result<Output, Failure>,
        concurrency: Int,
        ordered: Bool,
        bufferLimit: Int,
    ) {
        self.transform = transform
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
        self.ordered = ordered
        precondition(bufferLimit >= 0, "bufferLimit must be at least 0")
        self.bufferLimit = bufferLimit
    }

    @inlinable
    func attach(_ upstream: Pipe<Input, Failure>) -> Pipe<Output, Failure> {
        let step = AsyncFlatMapStep(transform)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        let ordered = self.ordered
        let bufferLimit = self.bufferLimit
        return .iterating {
            upstream.compactMapAsyncIterator(
                concurrency: concurrency,
                ordered: ordered,
                bufferLimit: bufferLimit,
                passthrough: { $0.passthroughFailure() },
                { element in await step(element) },
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

/// Changes each value with an async closure that returns a `Result`. Failures pass through
/// unchanged.
///
/// When the closure returns `.failure`, that element becomes a failure. `concurrency`,
/// `ordered` and `bufferLimit` work as in `AsyncMap`.
@inlinable
public func AsyncFlatMap<Input: Sendable, Output: Sendable, Failure: Error & Sendable>(
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    _ transform: @escaping @Sendable (Input) async -> Result<Output, Failure>,
) -> some PipeStage<Input, Output, Failure> {
    AsyncFlatMapStage(
        transform,
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit
    )
}

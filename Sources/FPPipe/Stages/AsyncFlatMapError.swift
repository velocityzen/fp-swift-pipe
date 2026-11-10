import FP

/// The stage that `AsyncFlatMapError` returns.
@usableFromInline
struct AsyncFlatMapErrorStage<
    Value: Sendable,
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable,
>: PipeFlatErrorStage {
    @usableFromInline let transform: @Sendable (InputFailure) async -> Result<Value, OutputFailure>
    @usableFromInline let concurrency: Int
    @usableFromInline let ordered: Bool
    @usableFromInline let bufferLimit: Int

    @inlinable
    init(
        _ transform: @escaping @Sendable (InputFailure) async -> Result<Value, OutputFailure>,
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
    func attach(_ upstream: Pipe<Value, InputFailure>) -> Pipe<Value, OutputFailure> {
        let step = AsyncFlatMapErrorStep(transform)
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
                passthrough: { $0.passthroughSuccess() },
                { element in await step(element) },
            )
        }
    }
}

@usableFromInline
struct AsyncFlatMapErrorStep<Value, InputFailure: Error, OutputFailure: Error>: AsyncPipeStep {
    @usableFromInline let transform: @Sendable (InputFailure) async -> Result<Value, OutputFailure>

    @inlinable
    init(
        _ transform: @escaping @Sendable (InputFailure) async -> Result<Value, OutputFailure>
    ) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(
        _ element: Result<Value, InputFailure>
    ) async -> Result<Value, OutputFailure>? {
        await element.orElseAsync(transform)
    }
}

/// Replaces each failure with the `Result` that an async closure makes from the error.
/// Successes pass through unchanged.
///
/// Return `.success` to recover, or `.failure` to fail again. The new error can have another
/// type. `concurrency`, `ordered` and `bufferLimit` work as in `AsyncMap`.
@inlinable
public func AsyncFlatMapError<
    Value: Sendable,
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable,
>(
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    _ transform: @escaping @Sendable (InputFailure) async -> Result<Value, OutputFailure>,
) -> some PipeFlatErrorStage<Value, InputFailure, OutputFailure> {
    AsyncFlatMapErrorStage(
        transform,
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit
    )
}

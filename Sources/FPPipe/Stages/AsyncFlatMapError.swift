import FP

/// Asynchronously replace each `.failure` with a `Result` computed from the error.
/// The output failure type may differ from the input. Internally delegates to fp-swift's
/// `Result.orElseAsync`.
///
/// `concurrency` controls how many recoveries run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered).
@usableFromInline
struct AsyncFlatMapErrorStage<
    Value: Sendable,
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable,
>: PipeFlatErrorStage {
    @usableFromInline let transform: @Sendable (InputFailure) async -> Result<Value, OutputFailure>
    @usableFromInline let concurrency: Int

    @inlinable
    init(
        _ transform: @escaping @Sendable (InputFailure) async -> Result<Value, OutputFailure>,
        concurrency: Int,
    ) {
        self.transform = transform
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, InputFailure>) -> Pipe<Value, OutputFailure> {
        let step = AsyncFlatMapErrorStep(transform)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        return .iterating {
            .init(
                upstream.compactMapAsyncUnordered(
                    concurrency: concurrency,
                    passthrough: { $0.passthroughSuccess() },
                    { element in await step(element) },
                )
                .makeAsyncIterator(),
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

/// DSL: `AsyncFlatMapError { (e: NetError) async -> Result<Item, AppError> in … }`.
@inlinable
public func AsyncFlatMapError<
    Value: Sendable,
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable,
>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (InputFailure) async -> Result<Value, OutputFailure>,
) -> some PipeFlatErrorStage<Value, InputFailure, OutputFailure> {
    AsyncFlatMapErrorStage(transform, concurrency: concurrency)
}

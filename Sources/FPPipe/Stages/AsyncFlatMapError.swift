import FP

/// Asynchronously replace each `.failure` with a `Result` computed from the error.
/// The output failure type may differ from the input. Internally delegates to fp-swift's
/// `Result.orElseAsync`.
///
/// `concurrency` controls how many recoveries run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered).
struct AsyncFlatMapErrorStage<
    Value: Sendable,
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable,
>: PipeFlatErrorStage {
    private let transform: @Sendable (InputFailure) async -> Result<Value, OutputFailure>
    private let concurrency: Int

    init(
        _ transform: @escaping @Sendable (InputFailure) async -> Result<Value, OutputFailure>,
        concurrency: Int,
    ) {
        self.transform = transform
        self.concurrency = max(1, concurrency)
    }

    func attach(_ upstream: Pipe<Value, InputFailure>) -> Pipe<Value, OutputFailure> {
        let transform = self.transform
        let concurrency = self.concurrency
        return .erased {
            let source = upstream.upstream()
            if concurrency == 1 {
                return AnyAsyncSequence(
                    source.map {
                        (element: Result<Value, InputFailure>) async -> Result<Value, OutputFailure>
                        in
                        await element.orElseAsync(transform)
                    },
                )
            }
            return AnyAsyncSequence(
                mapAsyncUnordered(source, concurrency: concurrency) {
                    (element: Result<Value, InputFailure>) async -> Result<Value, OutputFailure> in
                    await element.orElseAsync(transform)
                },
            )
        }
    }
}

/// DSL: `AsyncFlatMapError { (e: NetError) async -> Result<Item, AppError> in … }`.
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

import FP

/// Asynchronously replace each `.failure` with a `Success` derived from the error.
/// The output pipeline cannot fail (`Failure == Never`). Mirrors fp-swift's
/// `Result.getOrElseAsync`.
///
/// `concurrency` controls how many fallbacks run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered).
struct AsyncGetOrElseStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    typealias InputFailure = F
    typealias OutputFailure = Never

    private let onFailure: @Sendable (F) async -> Value
    private let concurrency: Int

    init(_ onFailure: @escaping @Sendable (F) async -> Value, concurrency: Int) {
        self.onFailure = onFailure
        self.concurrency = max(1, concurrency)
    }

    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, Never> {
        let onFailure = self.onFailure
        let concurrency = self.concurrency
        return .erased {
            let source = upstream.upstream()
            if concurrency == 1 {
                return AnyAsyncSequence(
                    source.map { (element: Result<Value, F>) async -> Result<Value, Never> in
                        .success(await element.getOrElseAsync(onFailure))
                    },
                )
            }
            return AnyAsyncSequence(
                mapAsyncUnordered(source, concurrency: concurrency) {
                    (element: Result<Value, F>) async -> Result<Value, Never> in
                    .success(await element.getOrElseAsync(onFailure))
                },
            )
        }
    }
}

/// DSL: `AsyncGetOrElse { (e: AppError) async in await fetchFallback() }`.
public func AsyncGetOrElse<Value: Sendable, F: Error & Sendable>(
    concurrency: Int = 1,
    _ onFailure: @escaping @Sendable (F) async -> Value,
) -> some PipeFlatErrorStage<Value, F, Never> {
    AsyncGetOrElseStage(onFailure, concurrency: concurrency)
}

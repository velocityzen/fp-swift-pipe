import FP

/// Asynchronously replace each `.failure` with an alternative `Result` that doesn't
/// see the error. Mirrors fp-swift's `Result.altAsync`.
///
/// `concurrency` controls how many fallbacks run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered).
struct AsyncAltStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    typealias InputFailure = F
    typealias OutputFailure = F

    private let alternative: @Sendable () async -> Result<Value, F>
    private let concurrency: Int

    init(
        _ alternative: @escaping @Sendable () async -> Result<Value, F>,
        concurrency: Int,
    ) {
        self.alternative = alternative
        self.concurrency = max(1, concurrency)
    }

    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let alternative = self.alternative
        let concurrency = self.concurrency
        return .erased {
            let source = upstream.upstream()
            if concurrency == 1 {
                return AnyAsyncSequence(
                    source.map { (element: Result<Value, F>) async -> Result<Value, F> in
                        await element.altAsync(alternative)
                    },
                )
            }
            return AnyAsyncSequence(
                mapAsyncUnordered(source, concurrency: concurrency) {
                    (element: Result<Value, F>) async -> Result<Value, F> in
                    await element.altAsync(alternative)
                },
            )
        }
    }
}

/// DSL: `AsyncAlt { await fetchFromCache() }`.
public func AsyncAlt<Value: Sendable, F: Error & Sendable>(
    concurrency: Int = 1,
    _ alternative: @escaping @Sendable () async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncAltStage(alternative, concurrency: concurrency)
}

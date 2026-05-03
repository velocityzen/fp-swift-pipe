import FP

/// Asynchronously folds each `Result` element into a single `Output` type.
/// Mirrors fp-swift's `Result.matchAsync`.
///
/// `concurrency` controls how many folds run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered).
struct AsyncMatchStage<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>:
    PipeFoldStage
{
    private let onSuccess: @Sendable (Input) async -> Output
    private let onFailure: @Sendable (InputFailure) async -> Output
    private let concurrency: Int

    init(
        onSuccess: @escaping @Sendable (Input) async -> Output,
        onFailure: @escaping @Sendable (InputFailure) async -> Output,
        concurrency: Int,
    ) {
        self.onSuccess = onSuccess
        self.onFailure = onFailure
        self.concurrency = max(1, concurrency)
    }

    func attach(_ upstream: Pipe<Input, InputFailure>) -> Pipe<Output, Never> {
        let onSuccess = self.onSuccess
        let onFailure = self.onFailure
        let concurrency = self.concurrency
        return .erased {
            let source = upstream.upstream()
            if concurrency == 1 {
                return AnyAsyncSequence(
                    source.map {
                        (element: Result<Input, InputFailure>) async -> Result<Output, Never> in
                        .success(await element.matchAsync(onSuccess, onFailure))
                    },
                )
            }
            return AnyAsyncSequence(
                mapAsyncUnordered(source, concurrency: concurrency) {
                    (element: Result<Input, InputFailure>) async -> Result<Output, Never> in
                    .success(await element.matchAsync(onSuccess, onFailure))
                },
            )
        }
    }
}

/// DSL: `AsyncMatch(onSuccess: { item in await format(item) }, onFailure: { e in await report(e) })`.
public func AsyncMatch<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>(
    concurrency: Int = 1,
    onSuccess: @escaping @Sendable (Input) async -> Output,
    onFailure: @escaping @Sendable (InputFailure) async -> Output,
) -> some PipeFoldStage<Input, Output, InputFailure> {
    AsyncMatchStage(onSuccess: onSuccess, onFailure: onFailure, concurrency: concurrency)
}

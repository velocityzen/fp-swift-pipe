import FP

/// Folds each `Result` element into a single `Output` type by handling both success
/// and failure cases. Output pipeline cannot fail (`Failure == Never`).
/// Mirrors fp-swift's `Result.match`.
struct MatchStage<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>:
    PipeFoldStage
{
    private let onSuccess: @Sendable (Input) -> Output
    private let onFailure: @Sendable (InputFailure) -> Output

    init(
        onSuccess: @escaping @Sendable (Input) -> Output,
        onFailure: @escaping @Sendable (InputFailure) -> Output,
    ) {
        self.onSuccess = onSuccess
        self.onFailure = onFailure
    }

    func attach(_ upstream: Pipe<Input, InputFailure>) -> Pipe<Output, Never> {
        let onSuccess = self.onSuccess
        let onFailure = self.onFailure
        return .erased {
            AnyAsyncSequence(
                upstream.upstream().map {
                    (element: Result<Input, InputFailure>) -> Result<Output, Never> in
                    .success(element.match(onSuccess, onFailure))
                },
            )
        }
    }
}

/// DSL: `Match(onSuccess: { item in "ok: \(item)" }, onFailure: { e in "err: \(e)" })`.
///
/// Both sides must be closures — autoclosure-default variants don't work as
/// builder stages because the factory can't infer `Input` / `InputFailure` from
/// non-closure arguments alone.
public func Match<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>(
    onSuccess: @escaping @Sendable (Input) -> Output,
    onFailure: @escaping @Sendable (InputFailure) -> Output,
) -> some PipeFoldStage<Input, Output, InputFailure> {
    MatchStage(onSuccess: onSuccess, onFailure: onFailure)
}

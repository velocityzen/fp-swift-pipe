import FP

/// Folds each `Result` element into a single `Output` type by handling both success
/// and failure cases. Output pipeline cannot fail (`Failure == Never`).
/// Mirrors fp-swift's `Result.match`.
@usableFromInline
struct MatchStage<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>:
    PipeFoldStage
{
    @usableFromInline let onSuccess: @Sendable (Input) -> Output
    @usableFromInline let onFailure: @Sendable (InputFailure) -> Output

    @inlinable
    init(
        onSuccess: @escaping @Sendable (Input) -> Output,
        onFailure: @escaping @Sendable (InputFailure) -> Output,
    ) {
        self.onSuccess = onSuccess
        self.onFailure = onFailure
    }

    @inlinable
    func attach(_ upstream: Pipe<Input, InputFailure>) -> Pipe<Output, Never> {
        let step = MatchStep(onSuccess: onSuccess, onFailure: onFailure)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct MatchStep<Input, Output, InputFailure: Error>: PipeStep {
    @usableFromInline let onSuccess: @Sendable (Input) -> Output
    @usableFromInline let onFailure: @Sendable (InputFailure) -> Output

    @inlinable
    init(
        onSuccess: @escaping @Sendable (Input) -> Output,
        onFailure: @escaping @Sendable (InputFailure) -> Output,
    ) {
        self.onSuccess = onSuccess
        self.onFailure = onFailure
    }

    @inlinable
    func callAsFunction(_ element: Result<Input, InputFailure>) -> Result<Output, Never>? {
        .success(element.match(onSuccess, onFailure))
    }
}

/// DSL: `Match(onSuccess: { item in "ok: \(item)" }, onFailure: { e in "err: \(e)" })`.
///
/// Both sides must be closures — autoclosure-default variants don't work as
/// builder stages because the factory can't infer `Input` / `InputFailure` from
/// non-closure arguments alone.
@inlinable
public func Match<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>(
    onSuccess: @escaping @Sendable (Input) -> Output,
    onFailure: @escaping @Sendable (InputFailure) -> Output,
) -> some PipeFoldStage<Input, Output, InputFailure> {
    MatchStage(onSuccess: onSuccess, onFailure: onFailure)
}

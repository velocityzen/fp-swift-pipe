import FP

/// The stage behind `Match(onSuccess:onFailure:)`.
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

/// Turns every element into one type: successes with `onSuccess`, failures with `onFailure`.
///
/// The pipe cannot fail after it. See <doc:HandlingFailures>.
///
/// ```swift
/// Match(
///     onSuccess: { (item: String) in "ok: \(item)" },
///     onFailure: { (error: FetchError) in "error: \(error)" }
/// )
/// ```
@inlinable
public func Match<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>(
    onSuccess: @escaping @Sendable (Input) -> Output,
    onFailure: @escaping @Sendable (InputFailure) -> Output,
) -> some PipeFoldStage<Input, Output, InputFailure> {
    MatchStage(onSuccess: onSuccess, onFailure: onFailure)
}

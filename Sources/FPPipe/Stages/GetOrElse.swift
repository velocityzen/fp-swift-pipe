import FP

/// Replace each `.failure` with a `Success` derived from the error. The output
/// pipeline cannot fail (`Failure == Never`). Mirrors fp-swift's `Result.getOrElse`.
@usableFromInline
struct GetOrElseStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = Never

    @usableFromInline let onFailure: @Sendable (F) -> Value

    @inlinable
    init(_ onFailure: @escaping @Sendable (F) -> Value) {
        self.onFailure = onFailure
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, Never> {
        let step = GetOrElseStep(onFailure)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct GetOrElseStep<Value, F: Error>: PipeStep {
    @usableFromInline let onFailure: @Sendable (F) -> Value

    @inlinable
    init(_ onFailure: @escaping @Sendable (F) -> Value) {
        self.onFailure = onFailure
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) -> Result<Value, Never>? {
        .success(element.getOrElse(onFailure))
    }
}

/// DSL: `GetOrElse { (e: AppError) in fallbackItem }`.
@inlinable
public func GetOrElse<Value: Sendable, F: Error & Sendable>(
    _ onFailure: @escaping @Sendable (F) -> Value,
) -> some PipeFlatErrorStage<Value, F, Never> {
    GetOrElseStage(onFailure)
}

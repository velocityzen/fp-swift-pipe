import FP

/// The stage that `GetOrElse` returns.
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

/// Replaces each failure with a value that the closure makes from the error.
///
/// After this stage, the pipe cannot fail.
@inlinable
public func GetOrElse<Value: Sendable, F: Error & Sendable>(
    _ onFailure: @escaping @Sendable (F) -> Value,
) -> some PipeFlatErrorStage<Value, F, Never> {
    GetOrElseStage(onFailure)
}

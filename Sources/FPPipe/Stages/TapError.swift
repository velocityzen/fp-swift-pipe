import FP

/// Observes failures and passes them through unchanged. Successes are not observed.
///
/// Value-polymorphic and failure-bound: `InputFailure == OutputFailure`.
@usableFromInline
struct TapErrorStage<F: Error & Sendable>: PipePolyValueStage {
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

    @usableFromInline let action: @Sendable (F) -> Void

    @inlinable
    init(_ action: @escaping @Sendable (F) -> Void) {
        self.action = action
    }

    @inlinable
    func attach<V: Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let step = TapErrorStep<V, F>(action)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct TapErrorStep<Value, F: Error>: PipeStep {
    @usableFromInline let action: @Sendable (F) -> Void

    @inlinable
    init(_ action: @escaping @Sendable (F) -> Void) {
        self.action = action
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) -> Result<Value, F>? {
        element.tapError(action)
    }
}

/// DSL: `TapError { (e: AppError) in log(e) }`.
@inlinable
public func TapError<F: Error & Sendable>(
    _ action: @escaping @Sendable (F) -> Void,
) -> some PipePolyValueStage<F, F> {
    TapErrorStage(action)
}

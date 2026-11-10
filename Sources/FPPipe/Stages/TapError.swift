import FP

/// The stage that `TapError` returns.
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

/// Runs a side effect for each failure, like logging. The failure does not change.
///
/// Successes pass through unchanged.
@inlinable
public func TapError<F: Error & Sendable>(
    _ action: @escaping @Sendable (F) -> Void,
) -> some PipePolyValueStage<F, F> {
    TapErrorStage(action)
}

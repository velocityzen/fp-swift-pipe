import FP

/// Observes successful values and passes them through unchanged. Failures are not
/// observed by this stage.
@usableFromInline
struct TapStage<Value: Sendable>: PipePolyStage {
    @usableFromInline typealias Input = Value
    @usableFromInline typealias Output = Value

    @usableFromInline let action: @Sendable (Value) -> Void

    @inlinable
    init(_ action: @escaping @Sendable (Value) -> Void) {
        self.action = action
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = TapStep<Value, F>(action)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct TapStep<Value, F: Error>: PipeStep {
    @usableFromInline let action: @Sendable (Value) -> Void

    @inlinable
    init(_ action: @escaping @Sendable (Value) -> Void) {
        self.action = action
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) -> Result<Value, F>? {
        element.tap(action)
    }
}

/// DSL: `Tap { value in print(value) }`.
@inlinable
public func Tap<Value: Sendable>(
    _ action: @escaping @Sendable (Value) -> Void,
) -> some PipePolyStage<Value, Value> {
    TapStage(action)
}

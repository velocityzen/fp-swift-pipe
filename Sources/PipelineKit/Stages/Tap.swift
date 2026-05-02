import FP

/// Observes successful values and passes them through unchanged. Failures are not
/// observed by this stage.
struct TapStage<Value: Sendable>: PipePolyStage {
    typealias Input = Value
    typealias Output = Value

    private let action: @Sendable (Value) -> Void

    init(_ action: @escaping @Sendable (Value) -> Void) {
        self.action = action
    }

    func attach<F: Error & Sendable>(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let action = self.action
        return .erased { AnyAsyncSequence(upstream.upstream().tap(action)) }
    }
}

/// DSL: `Tap { value in print(value) }`.
public func Tap<Value: Sendable>(
    _ action: @escaping @Sendable (Value) -> Void,
) -> some PipePolyStage<Value, Value> {
    TapStage(action)
}

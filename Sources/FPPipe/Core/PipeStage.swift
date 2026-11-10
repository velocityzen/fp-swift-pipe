/// A stage that fixes both the value type and the failure type.
///
/// `FlatMap` and `Match(pattern)` are examples. See <doc:Design>.
public protocol PipeStage<Input, Output, Failure>: Sendable {
    associatedtype Input: Sendable
    associatedtype Output: Sendable
    associatedtype Failure: Error & Sendable

    func attach(_ upstream: Pipe<Input, Failure>) -> Pipe<Output, Failure>
}

/// A stage that fixes the value type and leaves the failure type open.
///
/// Failures pass through unchanged. `Map`, `Filter` and `Tap` are examples.
public protocol PipePolyStage<Input, Output>: Sendable {
    associatedtype Input: Sendable
    associatedtype Output: Sendable

    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F>
}

/// A stage that fixes the failure type and leaves the value type open.
///
/// Values pass through unchanged. `MapError` and `TapError` are examples.
public protocol PipePolyValueStage<InputFailure, OutputFailure>: Sendable {
    associatedtype InputFailure: Error & Sendable
    associatedtype OutputFailure: Error & Sendable

    func attach<V: Sendable>(_ upstream: Pipe<V, InputFailure>) -> Pipe<V, OutputFailure>
}

/// A stage that leaves both the value type and the failure type open.
///
/// The element types do not change. `Take` and `Drop` are examples.
public protocol PipeForwardingStage: Sendable {
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F>
}

/// A stage that fixes the value type and can change the failure type.
///
/// It turns a failure into a value, or into a new failure. `FlatMapError`, `Alt` and
/// `GetOrElse` are examples.
public protocol PipeFlatErrorStage<Value, InputFailure, OutputFailure>: Sendable {
    associatedtype Value: Sendable
    associatedtype InputFailure: Error & Sendable
    associatedtype OutputFailure: Error & Sendable

    func attach(_ upstream: Pipe<Value, InputFailure>) -> Pipe<Value, OutputFailure>
}

/// A stage that turns values and failures into values of one new type.
///
/// After it, the pipe cannot fail. `Match(onSuccess:onFailure:)` is an example.
public protocol PipeFoldStage<Input, Output, InputFailure>: Sendable {
    associatedtype Input: Sendable
    associatedtype Output: Sendable
    associatedtype InputFailure: Error & Sendable

    func attach(_ upstream: Pipe<Input, InputFailure>) -> Pipe<Output, Never>
}

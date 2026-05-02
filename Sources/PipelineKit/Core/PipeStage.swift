/// A stage that operates within a specific failure channel — i.e. it inspects,
/// transforms, or produces values of a known `Failure` type. Examples: `FlatMap`,
/// `AsyncFlatMap`.
public protocol PipeStage<Input, Output, Failure>: Sendable {
    associatedtype Input: Sendable
    associatedtype Output: Sendable
    associatedtype Failure: Error & Sendable

    func attach(_ upstream: Pipe<Input, Failure>) -> Pipe<Output, Failure>
}

/// A stage that is polymorphic in the failure channel — it merely threads errors
/// through unchanged. Examples: `Map`, `Tap`, `Filter`. The upstream's failure
/// type is preserved by `attach`.
public protocol PipePolyStage<Input, Output>: Sendable {
    associatedtype Input: Sendable
    associatedtype Output: Sendable

    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F>
}

/// A stage that operates on the failure channel and is polymorphic in the value
/// channel — the success type is threaded through unchanged. Examples: `MapError`,
/// `TapError`. Set `InputFailure == OutputFailure` for failure-observing stages.
public protocol PipePolyValueStage<InputFailure, OutputFailure>: Sendable {
    associatedtype InputFailure: Error & Sendable
    associatedtype OutputFailure: Error & Sendable

    func attach<V: Sendable>(_ upstream: Pipe<V, InputFailure>) -> Pipe<V, OutputFailure>
}

/// A stage that observes neither value nor failure type — both channels are threaded
/// through unchanged. Examples: `Take`, `Drop`. Useful for control-flow operators that
/// don't depend on the carried types.
public protocol PipeForwardingStage: Sendable {
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F>
}

/// A stage that bind-transforms the failure channel — its closure can either recover
/// to a success or re-fail with a different error type. The success type is preserved.
/// Examples: `FlatMapError`, `AsyncFlatMapError`.
///
/// Distinct from `PipePolyValueStage` because the closure's return type references
/// the upstream's value type (`Result<Value, OutputFailure>`), so `Value` must be bound
/// at construction rather than method-generic.
public protocol PipeFlatErrorStage<Value, InputFailure, OutputFailure>: Sendable {
    associatedtype Value: Sendable
    associatedtype InputFailure: Error & Sendable
    associatedtype OutputFailure: Error & Sendable

    func attach(_ upstream: Pipe<Value, InputFailure>) -> Pipe<Value, OutputFailure>
}

/// A stage that folds `Result`-bearing elements into a single output type. Both successes
/// and failures collapse to `Output`; the output pipeline cannot fail. Examples: `Match`,
/// `AsyncMatch`.
///
/// This is the elimination operation on `Result`: it removes the failure channel entirely
/// by handling both cases.
public protocol PipeFoldStage<Input, Output, InputFailure>: Sendable {
    associatedtype Input: Sendable
    associatedtype Output: Sendable
    associatedtype InputFailure: Error & Sendable

    func attach(_ upstream: Pipe<Input, InputFailure>) -> Pipe<Output, Never>
}

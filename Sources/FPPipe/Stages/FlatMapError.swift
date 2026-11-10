import FP

/// The stage that `FlatMapError` returns.
@usableFromInline
struct FlatMapErrorStage<
    Value: Sendable,
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable,
>: PipeFlatErrorStage {
    @usableFromInline let transform: @Sendable (InputFailure) -> Result<Value, OutputFailure>

    @inlinable
    init(_ transform: @escaping @Sendable (InputFailure) -> Result<Value, OutputFailure>) {
        self.transform = transform
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, InputFailure>) -> Pipe<Value, OutputFailure> {
        let step = FlatMapErrorStep(transform)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct FlatMapErrorStep<Value, InputFailure: Error, OutputFailure: Error>: PipeStep {
    @usableFromInline let transform: @Sendable (InputFailure) -> Result<Value, OutputFailure>

    @inlinable
    init(_ transform: @escaping @Sendable (InputFailure) -> Result<Value, OutputFailure>) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, InputFailure>) -> Result<Value, OutputFailure>? {
        element.orElse(transform)
    }
}

/// Replaces each failure with the `Result` that the closure makes from the error. Successes
/// pass through unchanged.
///
/// Return `.success` to recover, or `.failure` to fail again. The new error can have another
/// type. To handle only some errors, use `MatchError`.
@inlinable
public func FlatMapError<
    Value: Sendable,
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable
>(
    _ transform: @escaping @Sendable (InputFailure) -> Result<Value, OutputFailure>,
) -> some PipeFlatErrorStage<Value, InputFailure, OutputFailure> {
    FlatMapErrorStage(transform)
}

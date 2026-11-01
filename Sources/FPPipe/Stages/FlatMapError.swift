import FP

/// Replace each `.failure` with the `Result` produced by the closure. The output
/// failure type may differ from the input — yield `.success` to recover, `.failure`
/// to re-fail with a (possibly different) error. Internally delegates to fp-swift's
/// `Result.orElse`.
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

/// DSL: `FlatMapError { (e: NetError) -> Result<Item, AppError> in … }`.
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

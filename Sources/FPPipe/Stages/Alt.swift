import FP

/// Replace each `.failure` with an alternative `Result` that doesn't see the error.
/// The leftmost success wins; if both are failures, the alternative's failure is
/// propagated. Mirrors fp-swift's `Result.alt`.
@usableFromInline
struct AltStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

    @usableFromInline let alternative: @Sendable () -> Result<Value, F>

    @inlinable
    init(_ alternative: @escaping @Sendable () -> Result<Value, F>) {
        self.alternative = alternative
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = AltStep(alternative)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct AltStep<Value, F: Error>: PipeStep {
    @usableFromInline let alternative: @Sendable () -> Result<Value, F>

    @inlinable
    init(_ alternative: @escaping @Sendable () -> Result<Value, F>) {
        self.alternative = alternative
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) -> Result<Value, F>? {
        element.alt(alternative)
    }
}

/// DSL: `Alt { Result<Item, AppError>.success(.placeholder) }`.
@inlinable
public func Alt<Value: Sendable, F: Error & Sendable>(
    _ alternative: @escaping @Sendable () -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AltStage(alternative)
}

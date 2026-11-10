import FP

/// The stage that `Alt` returns.
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

/// Replaces each failure with the `Result` that the closure returns.
///
/// The closure does not see the error. Successes pass through unchanged.
///
/// ```swift
/// Alt { Result<Int, ParseError>.success(0) }
/// ```
@inlinable
public func Alt<Value: Sendable, F: Error & Sendable>(
    _ alternative: @escaping @Sendable () -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AltStage(alternative)
}

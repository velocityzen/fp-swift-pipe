import FP

/// Replace each `.failure` with an alternative `Result` that doesn't see the error.
/// The leftmost success wins; if both are failures, the alternative's failure is
/// propagated. Mirrors fp-swift's `Result.alt`.
struct AltStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    typealias InputFailure = F
    typealias OutputFailure = F

    private let alternative: @Sendable () -> Result<Value, F>

    init(_ alternative: @escaping @Sendable () -> Result<Value, F>) {
        self.alternative = alternative
    }

    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let alternative = self.alternative
        return .erased {
            AnyAsyncSequence(
                upstream.upstream().map { (element: Result<Value, F>) -> Result<Value, F> in
                    element.alt(alternative)
                },
            )
        }
    }
}

/// DSL: `Alt { Result<Item, AppError>.success(.placeholder) }`.
public func Alt<Value: Sendable, F: Error & Sendable>(
    _ alternative: @escaping @Sendable () -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AltStage(alternative)
}

import FP

/// Observes failures and passes them through unchanged. Successes are not observed.
///
/// Value-polymorphic and failure-bound: `InputFailure == OutputFailure`.
struct TapErrorStage<F: Error & Sendable>: PipePolyValueStage {
    typealias InputFailure = F
    typealias OutputFailure = F

    private let action: @Sendable (F) -> Void

    init(_ action: @escaping @Sendable (F) -> Void) {
        self.action = action
    }

    func attach<V: Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let action = self.action
        return .erased { AnyAsyncSequence(upstream.upstream().tapError(action)) }
    }
}

/// DSL: `TapError { (e: AppError) in log(e) }`.
public func TapError<F: Error & Sendable>(
    _ action: @escaping @Sendable (F) -> Void,
) -> some PipePolyValueStage<F, F> {
    TapErrorStage(action)
}

import FP

/// Asynchronously observes failures and passes them through unchanged.
/// Successes are not observed.
struct AsyncTapErrorStage<F: Error & Sendable>: PipePolyValueStage {
    typealias InputFailure = F
    typealias OutputFailure = F

    private let action: @Sendable (F) async -> Void

    init(_ action: @escaping @Sendable (F) async -> Void) {
        self.action = action
    }

    func attach<V: Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let action = self.action
        return .erased { AnyAsyncSequence(upstream.upstream().tapErrorAsync(action)) }
    }
}

/// DSL: `AsyncTapError { (e: AppError) in await report(e) }`.
public func AsyncTapError<F: Error & Sendable>(
    _ action: @escaping @Sendable (F) async -> Void,
) -> some PipePolyValueStage<F, F> {
    AsyncTapErrorStage(action)
}

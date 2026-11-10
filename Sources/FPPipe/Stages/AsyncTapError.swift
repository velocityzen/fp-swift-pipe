/// The stage that `AsyncTapError` returns.
@usableFromInline
struct AsyncTapErrorStage<F: Error & Sendable>: PipePolyValueStage {
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

    @usableFromInline let action: @Sendable (F) async -> Void

    @inlinable
    init(_ action: @escaping @Sendable (F) async -> Void) {
        self.action = action
    }

    @inlinable
    func attach<V: Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let step = AsyncTapErrorStep<V, F>(action)
        return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct AsyncTapErrorStep<Value, F: Error>: AsyncPipeStep {
    @usableFromInline let action: @Sendable (F) async -> Void

    @inlinable
    init(_ action: @escaping @Sendable (F) async -> Void) {
        self.action = action
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) async -> Result<Value, F>? {
        if case .failure(let error) = element {
            await action(error)
        }
        return element
    }
}

/// Runs an async side effect for each failure, like logging. The failure does not change.
///
/// Successes pass through unchanged. The closure handles one failure at a time, so side
/// effects happen in order.
@inlinable
public func AsyncTapError<F: Error & Sendable>(
    _ action: @escaping @Sendable (F) async -> Void,
) -> some PipePolyValueStage<F, F> {
    AsyncTapErrorStage(action)
}

/// Asynchronously observes failures and passes them through unchanged.
/// Successes are not observed.
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

/// DSL: `AsyncTapError { (e: AppError) in await report(e) }`.
@inlinable
public func AsyncTapError<F: Error & Sendable>(
    _ action: @escaping @Sendable (F) async -> Void,
) -> some PipePolyValueStage<F, F> {
    AsyncTapErrorStage(action)
}

/// Asynchronously observes successful values and passes them through unchanged.
/// Failures are not observed by this stage.
@usableFromInline
struct AsyncTapStage<Value: Sendable>: PipePolyStage {
    @usableFromInline typealias Input = Value
    @usableFromInline typealias Output = Value

    @usableFromInline let action: @Sendable (Value) async -> Void

    @inlinable
    init(_ action: @escaping @Sendable (Value) async -> Void) {
        self.action = action
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = AsyncTapStep<Value, F>(action)
        return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct AsyncTapStep<Value, F: Error>: AsyncPipeStep {
    @usableFromInline let action: @Sendable (Value) async -> Void

    @inlinable
    init(_ action: @escaping @Sendable (Value) async -> Void) {
        self.action = action
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) async -> Result<Value, F>? {
        if case .success(let value) = element {
            await action(value)
        }
        return element
    }
}

/// DSL: `AsyncTap { value in await log(value) }`.
@inlinable
public func AsyncTap<Value: Sendable>(
    _ action: @escaping @Sendable (Value) async -> Void,
) -> some PipePolyStage<Value, Value> {
    AsyncTapStage(action)
}

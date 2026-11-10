/// The stage that `AsyncTap` returns.
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

/// Runs an async side effect for each value, like logging. The value does not change.
///
/// Failures pass through unchanged. The closure handles one value at a time, so side effects
/// happen in order.
@inlinable
public func AsyncTap<Value: Sendable>(
    _ action: @escaping @Sendable (Value) async -> Void,
) -> some PipePolyStage<Value, Value> {
    AsyncTapStage(action)
}

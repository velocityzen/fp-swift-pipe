/// The stage that `Filter` returns.
@usableFromInline
struct FilterStage<Value: Sendable>: PipePolyStage {
    @usableFromInline typealias Input = Value
    @usableFromInline typealias Output = Value

    @usableFromInline let predicate: @Sendable (Value) -> Bool

    @inlinable
    init(_ predicate: @escaping @Sendable (Value) -> Bool) {
        self.predicate = predicate
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = FilterStep<Value, F>(predicate)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct FilterStep<Value, F: Error>: PipeStep {
    @usableFromInline let predicate: @Sendable (Value) -> Bool

    @inlinable
    init(_ predicate: @escaping @Sendable (Value) -> Bool) {
        self.predicate = predicate
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) -> Result<Value, F>? {
        switch element {
            case .failure: element
            case .success(let value): predicate(value) ? element : nil
        }
    }
}

/// Keeps the values for which the closure returns `true`. Failures pass through unchanged.
@inlinable
public func Filter<Value: Sendable>(
    _ predicate: @escaping @Sendable (Value) -> Bool,
) -> some PipePolyStage<Value, Value> {
    FilterStage(predicate)
}

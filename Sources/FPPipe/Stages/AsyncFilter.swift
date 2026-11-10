import FP

/// The stage that `AsyncFilter` returns.
@usableFromInline
struct AsyncFilterStage<Value: Sendable>: PipePolyStage {
    @usableFromInline typealias Input = Value
    @usableFromInline typealias Output = Value

    @usableFromInline let predicate: @Sendable (Value) async -> Bool
    @usableFromInline let concurrency: Int
    @usableFromInline let ordered: Bool
    @usableFromInline let bufferLimit: Int

    @inlinable
    init(
        _ predicate: @escaping @Sendable (Value) async -> Bool,
        concurrency: Int,
        ordered: Bool,
        bufferLimit: Int
    ) {
        self.predicate = predicate
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
        self.ordered = ordered
        precondition(bufferLimit >= 0, "bufferLimit must be at least 0")
        self.bufferLimit = bufferLimit
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = AsyncFilterStep<Value, F>(predicate)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        let ordered = self.ordered
        let bufferLimit = self.bufferLimit
        return .iterating {
            upstream.compactMapAsyncIterator(
                concurrency: concurrency,
                ordered: ordered,
                bufferLimit: bufferLimit,
                passthrough: { $0.passthroughFailure() },
                { element in await step(element) },
            )
        }
    }
}

@usableFromInline
struct AsyncFilterStep<Value, F: Error>: AsyncPipeStep {
    @usableFromInline let predicate: @Sendable (Value) async -> Bool

    @inlinable
    init(_ predicate: @escaping @Sendable (Value) async -> Bool) {
        self.predicate = predicate
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) async -> Result<Value, F>? {
        switch element {
            case .failure: return element
            case .success(let value): return await predicate(value) ? element : nil
        }
    }
}

/// Keeps the values for which an async closure returns `true`. Failures pass through
/// unchanged.
///
/// `concurrency`, `ordered` and `bufferLimit` work as in `AsyncMap`.
@inlinable
public func AsyncFilter<Value: Sendable>(
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    _ predicate: @escaping @Sendable (Value) async -> Bool,
) -> some PipePolyStage<Value, Value> {
    AsyncFilterStage(
        predicate,
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit
    )
}

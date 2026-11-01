import FP

/// Asynchronously keeps only successes that satisfy the predicate. Failures pass through.
///
/// `concurrency` controls how many predicate evaluations run in parallel. Default 1 is
/// strictly sequential. With `concurrency > 1` up to N predicates run in flight and
/// results emit as they resolve (unordered).
@usableFromInline
struct AsyncFilterStage<Value: Sendable>: PipePolyStage {
    @usableFromInline typealias Input = Value
    @usableFromInline typealias Output = Value

    @usableFromInline let predicate: @Sendable (Value) async -> Bool
    @usableFromInline let concurrency: Int

    @inlinable
    init(_ predicate: @escaping @Sendable (Value) async -> Bool, concurrency: Int) {
        self.predicate = predicate
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = AsyncFilterStep<Value, F>(predicate)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        return .iterating {
            .init(
                upstream.compactMapAsyncUnordered(
                    concurrency: concurrency,
                    passthrough: { $0.passthroughFailure() },
                    { element in await step(element) },
                )
                .makeAsyncIterator(),
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

/// DSL: `AsyncFilter { value in await isInteresting(value) }` or
/// `AsyncFilter(concurrency: 10) { value in await isInteresting(value) }` for parallel evaluation.
@inlinable
public func AsyncFilter<Value: Sendable>(
    concurrency: Int = 1,
    _ predicate: @escaping @Sendable (Value) async -> Bool,
) -> some PipePolyStage<Value, Value> {
    AsyncFilterStage(predicate, concurrency: concurrency)
}

/// Asynchronously keeps only successes that satisfy the predicate. Failures pass through.
///
/// `concurrency` controls how many predicate evaluations run in parallel. Default 1 is
/// strictly sequential. With `concurrency > 1` up to N predicates run in flight and
/// results emit as they resolve (unordered).
struct AsyncFilterStage<Value: Sendable>: PipePolyStage {
    typealias Input = Value
    typealias Output = Value

    private let predicate: @Sendable (Value) async -> Bool
    private let concurrency: Int

    init(_ predicate: @escaping @Sendable (Value) async -> Bool, concurrency: Int) {
        self.predicate = predicate
        self.concurrency = max(1, concurrency)
    }

    func attach<F: Error & Sendable>(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let predicate = self.predicate
        let concurrency = self.concurrency
        return .erased {
            let source = upstream.upstream()
            if concurrency == 1 {
                return AnyAsyncSequence(
                    source.compactMap { (element: Result<Value, F>) -> Result<Value, F>? in
                        switch element {
                            case .failure: return element
                            case .success(let value): return await predicate(value) ? element : nil
                        }
                    },
                )
            }
            return AnyAsyncSequence(
                compactMapAsyncUnordered(source, concurrency: concurrency) {
                    (element: Result<Value, F>) async -> Result<Value, F>? in
                    switch element {
                        case .failure: return element
                        case .success(let value): return await predicate(value) ? element : nil
                    }
                },
            )
        }
    }
}

/// DSL: `AsyncFilter { value in await isInteresting(value) }` or
/// `AsyncFilter(concurrency: 10) { value in await isInteresting(value) }` for parallel evaluation.
public func AsyncFilter<Value: Sendable>(
    concurrency: Int = 1,
    _ predicate: @escaping @Sendable (Value) async -> Bool,
) -> some PipePolyStage<Value, Value> {
    AsyncFilterStage(predicate, concurrency: concurrency)
}

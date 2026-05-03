/// Keeps only successes that satisfy the predicate. Failures pass through unchanged.
struct FilterStage<Value: Sendable>: PipePolyStage {
    typealias Input = Value
    typealias Output = Value

    private let predicate: @Sendable (Value) -> Bool

    init(_ predicate: @escaping @Sendable (Value) -> Bool) {
        self.predicate = predicate
    }

    func attach<F: Error & Sendable>(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let predicate = self.predicate
        return .erased {
            AnyAsyncSequence(
                upstream.upstream().compactMap { (element: Result<Value, F>) -> Result<Value, F>? in
                    switch element {
                        case .failure: return element
                        case .success(let value): return predicate(value) ? element : nil
                    }
                },
            )
        }
    }
}

/// DSL: `Filter { $0.isInteresting }`.
public func Filter<Value: Sendable>(
    _ predicate: @escaping @Sendable (Value) -> Bool,
) -> some PipePolyStage<Value, Value> {
    FilterStage(predicate)
}

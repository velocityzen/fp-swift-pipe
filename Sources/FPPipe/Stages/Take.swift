/// Forwards only the first `count` elements of the pipeline (successes and failures alike).
///
/// Counts every element regardless of `Result` kind — matches stdlib `AsyncSequence.prefix(_:)`.
/// To take the first N successes, compose `Filter { … } → Take(N)`; failures still consume
/// the budget when intermixed.
struct TakeStage: PipeForwardingStage {
    private let count: Int

    init(_ count: Int) {
        precondition(count >= 0, "Take count must be non-negative")
        self.count = count
    }

    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let count = self.count
        return .erased { AnyAsyncSequence(upstream.upstream().prefix(count)) }
    }
}

/// DSL: `Take(10)`.
public func Take(_ count: Int) -> some PipeForwardingStage {
    TakeStage(count)
}

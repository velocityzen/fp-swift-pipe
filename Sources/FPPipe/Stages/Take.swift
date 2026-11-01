/// Forwards only the first `count` elements of the pipeline (successes and failures alike).
///
/// Counts every element regardless of `Result` kind — matches stdlib `AsyncSequence.prefix(_:)`.
/// To take the first N successes, compose `Filter { … } → Take(N)`; failures still consume
/// the budget when intermixed.
@usableFromInline
struct TakeStage: PipeForwardingStage {
    @usableFromInline let count: Int

    @inlinable
    init(_ count: Int) {
        precondition(count >= 0, "Take count must be non-negative")
        self.count = count
    }

    @inlinable
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let count = self.count
        return .iterating { .init(TakeIterator(upstream.makeAsyncIterator(), count: count)) }
    }
}

/// Yields the first `remaining` elements of `Base`, then finishes without reading more.
@usableFromInline
struct TakeIterator<Base: AsyncIteratorProtocol>: AsyncIteratorProtocol
where Base.Failure == Never {
    @usableFromInline var base: Base
    @usableFromInline var remaining: Int

    @inlinable
    init(_ base: Base, count: Int) {
        self.base = base
        self.remaining = count
    }

    @inlinable
    mutating func next() async -> Base.Element? {
        await next(isolation: #isolation)
    }

    @inlinable
    mutating func next(isolation actor: isolated (any Actor)?) async -> Base.Element? {
        guard remaining > 0 else { return nil }
        remaining -= 1
        return await base.next(isolation: actor)
    }
}

/// DSL: `Take(10)`.
@inlinable
public func Take(_ count: Int) -> some PipeForwardingStage {
    TakeStage(count)
}

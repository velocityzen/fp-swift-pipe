/// The stage that `Take` returns.
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

/// Gives the first `remaining` elements of `Base`, and then finishes without reading more.
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

/// Keeps the first `count` elements, and then stops reading.
///
/// Failures count too, like `prefix(_:)` on an `AsyncSequence`. `count` must be 0 or more.
@inlinable
public func Take(_ count: Int) -> some PipeForwardingStage {
    TakeStage(count)
}

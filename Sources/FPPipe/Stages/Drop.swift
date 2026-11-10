/// The stage that `Drop` returns.
@usableFromInline
struct DropStage: PipeForwardingStage {
    @usableFromInline let count: Int

    @inlinable
    init(_ count: Int) {
        precondition(count >= 0, "Drop count must be non-negative")
        self.count = count
    }

    @inlinable
    func attach<V: Sendable, F: Error & Sendable>(_ upstream: Pipe<V, F>) -> Pipe<V, F> {
        let count = self.count
        return .iterating { .init(DropIterator(upstream.makeAsyncIterator(), count: count)) }
    }
}

/// Skips the first `remaining` elements of `Base`, and then gives the rest.
@usableFromInline
struct DropIterator<Base: AsyncIteratorProtocol>: AsyncIteratorProtocol
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
        while remaining > 0 {
            remaining -= 1
            guard await base.next(isolation: actor) != nil else { return nil }
        }
        return await base.next(isolation: actor)
    }
}

/// Skips the first `count` elements.
///
/// Failures count too, like `dropFirst(_:)` on an `AsyncSequence`. `count` must be 0 or more.
@inlinable
public func Drop(_ count: Int) -> some PipeForwardingStage {
    DropStage(count)
}

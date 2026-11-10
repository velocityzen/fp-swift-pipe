/// A source that gives no elements and finishes at once.
@usableFromInline
struct EmptySource<V: Sendable, F: Error & Sendable>: PipeSource {
    @usableFromInline typealias Output = V
    @usableFromInline typealias Failure = F

    @inlinable
    init() {}

    @inlinable
    func produce() -> Pipe<V, F> {
        .iterating { .init(OneElementIterator<Result<V, F>>(nil)) }
    }
}

/// Gives no elements and finishes at once.
@inlinable
public func Empty<V: Sendable, F: Error & Sendable>(
    valueType _: V.Type,
    failureType _: F.Type,
) -> some PipeSource<V, F> {
    EmptySource()
}

/// Gives one element or none, and then finishes. Used by `Success`, `Failure` and `Empty`.
@usableFromInline
struct OneElementIterator<Element>: AsyncIteratorProtocol {
    @usableFromInline var element: Element?

    @inlinable
    init(_ element: Element?) {
        self.element = element
    }

    @inlinable
    mutating func next() async -> Element? {
        defer { element = nil }
        return element
    }

    @inlinable
    mutating func next(isolation actor: isolated (any Actor)?) async -> Element? {
        defer { element = nil }
        return element
    }
}

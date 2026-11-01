/// A source that finishes immediately with no elements. Both type parameters must
/// be supplied at the call site since no value is ever produced.
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

/// DSL: `Empty(valueType: Int.self, failureType: AppError.self)` — a source that finishes
/// without emitting. Both type parameters must be supplied since no value is ever produced.
@inlinable
public func Empty<V: Sendable, F: Error & Sendable>(
    valueType _: V.Type,
    failureType _: F.Type,
) -> some PipeSource<V, F> {
    EmptySource()
}

/// Yields its element, if it has one, then finishes — the iterator behind `Success`,
/// `Failure` and `Empty`.
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

/// An `AsyncSequence` that hides the type of the sequence it reads.
///
/// Each `makeAsyncIterator()` call builds a new iterator, so a pipe can run more than once.
public struct AnyAsyncSequence<Element: Sendable>: AsyncSequence, Sendable {
    public typealias AsyncIterator = Iterator

    @usableFromInline let _makeIterator: @Sendable () -> Iterator

    @inlinable
    public init<S: AsyncSequence & Sendable>(_ base: S)
    where S.Element == Element, S.Failure == Never {
        self._makeIterator = { Iterator(base.makeAsyncIterator()) }
    }

    @inlinable
    init(makeIterator: @escaping @Sendable () -> Iterator) {
        self._makeIterator = makeIterator
    }

    @inlinable
    public func makeAsyncIterator() -> Iterator {
        _makeIterator()
    }

    /// The iterator of an `AnyAsyncSequence`.
    public struct Iterator: AsyncIteratorProtocol {
        @usableFromInline var _next: () async -> Element?

        @inlinable
        init<I: AsyncIteratorProtocol>(_ base: I) where I.Element == Element, I.Failure == Never {
            var iterator = base
            self._next = {
                // Stays on the caller's isolation. It cannot throw, because `I.Failure == Never`.
                await iterator.next(isolation: #isolation)
            }
        }

        @inlinable
        public mutating func next() async -> Element? {
            await _next()
        }

        @inlinable
        public mutating func next(isolation actor: isolated (any Actor)?) async -> Element? {
            await _next()
        }
    }
}

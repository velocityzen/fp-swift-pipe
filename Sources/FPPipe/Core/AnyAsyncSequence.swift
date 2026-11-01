/// A type-erased `AsyncSequence` whose iteration is built lazily on demand.
///
/// Each call to `makeAsyncIterator()` invokes the stored builder, so the
/// underlying chain is reconstructed per iteration. This keeps `Pipe`
/// values re-iterable and free of shared mutable state.
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

    /// Erases a concrete iterator behind a closure. The closure is created where the
    /// iterator's type is known, so the code it runs is specialized for that type.
    public struct Iterator: AsyncIteratorProtocol {
        @usableFromInline var _next: () async -> Element?

        @inlinable
        init<I: AsyncIteratorProtocol>(_ base: I) where I.Element == Element, I.Failure == Never {
            var iterator = base
            self._next = {
                // Stays on the caller's isolation; `I.Failure == Never` makes it non-throwing.
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

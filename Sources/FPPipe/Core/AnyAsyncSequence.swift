/// A type-erased `AsyncSequence` whose iteration is built lazily on demand.
///
/// Each call to `makeAsyncIterator()` invokes the stored builder, so the
/// underlying chain is reconstructed per iteration. This keeps `Pipe`
/// values re-iterable and free of shared mutable state.
public struct AnyAsyncSequence<Element: Sendable>: AsyncSequence, Sendable {
    public typealias AsyncIterator = Iterator

    private let _makeIterator: @Sendable () -> Iterator

    public init<S: AsyncSequence & Sendable>(_ base: S)
    where S.Element == Element, S.Failure == Never {
        self._makeIterator = { Iterator(base) }
    }

    init(makeIterator: @escaping @Sendable () -> Iterator) {
        self._makeIterator = makeIterator
    }

    public func makeAsyncIterator() -> Iterator {
        _makeIterator()
    }

    public struct Iterator: AsyncIteratorProtocol {
        private var _next: () async -> Element?

        init<S: AsyncSequence>(_ base: S) where S.Element == Element, S.Failure == Never {
            var iterator = base.makeAsyncIterator()
            self._next = {
                // `try?` is provably safe given the `S.Failure == Never` constraint above —
                // it just satisfies the legacy untyped-throws `next()` signature.
                try? await iterator.next()
            }
        }

        public mutating func next() async -> Element? {
            await _next()
        }
    }
}

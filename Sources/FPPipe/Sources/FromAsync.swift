// MARK: - Source backing type

/// Defers a source behind an `async` constructor. On each iteration, `make` is awaited on
/// the first `next()` to produce an inner sequence, which is then read one element per
/// pull via the closure `makeNext` builds. The differing source shapes (async/sync
/// iteration × bare/Result element) all collapse into this single struct — the variation
/// lives entirely in `makeNext`.
///
/// Nothing is read ahead of the consumer: there is no producer task and no buffer, so a
/// huge inner sequence never lands in memory at once and `make` runs in the consumer's
/// task, on its isolation.
struct AsyncDeferredSource<Inner: Sendable, Output: Sendable, Failure: Error & Sendable>:
    PipeSource
{
    private let make: @Sendable () async -> Inner
    private let makeNext: @Sendable (Inner) -> () async -> Result<Output, Failure>?

    init(
        make: @escaping @Sendable () async -> Inner,
        makeNext: @escaping @Sendable (Inner) -> () async -> Result<Output, Failure>?,
    ) {
        self.make = make
        self.makeNext = makeNext
    }

    func produce() -> Pipe<Output, Failure> {
        let make = self.make
        let makeNext = self.makeNext
        return .erased {
            AnyAsyncSequence(DeferredSequence(make: make, makeNext: makeNext))
        }
    }
}

/// The lazily-started sequence behind `AsyncDeferredSource`.
struct DeferredSequence<Inner: Sendable, Element: Sendable>: AsyncSequence, Sendable {
    let make: @Sendable () async -> Inner
    let makeNext: @Sendable (Inner) -> () async -> Element?

    func makeAsyncIterator() -> Iterator {
        Iterator(make: make, makeNext: makeNext)
    }

    struct Iterator: AsyncIteratorProtocol {
        private enum State {
            case pending
            case running(() async -> Element?)
            case finished
        }

        private let make: @Sendable () async -> Inner
        private let makeNext: @Sendable (Inner) -> () async -> Element?
        private var state = State.pending

        init(
            make: @escaping @Sendable () async -> Inner,
            makeNext: @escaping @Sendable (Inner) -> () async -> Element?,
        ) {
            self.make = make
            self.makeNext = makeNext
        }

        mutating func next() async -> Element? {
            if case .pending = state {
                state = .running(makeNext(await make()))
            }
            guard case .running(let pull) = state, !Task.isCancelled,
                let element = await pull()
            else {
                state = .finished
                return nil
            }
            return element
        }
    }
}

// MARK: - DSL factories

/// Lift an async-produced `AsyncSequence` into a pipeline source. Each iteration
/// awaits `make` afresh — useful for sources that need async setup (cursors,
/// authenticated streams, network-bound preludes).
public func FromAsync<S: AsyncSequence & Sendable>(
    _ make: @escaping @Sendable () async -> S,
) -> some PipeSource<S.Element, Never>
where S.Element: Sendable, S.Failure == Never {
    AsyncDeferredSource<S, S.Element, Never>(
        make: make,
        makeNext: { inner in
            var iterator = inner.makeAsyncIterator()
            return { await iterator.next(isolation: #isolation).map(Result.success) }
        },
    )
}

/// Lift an async-produced synchronous `Sequence` into a pipeline source. Each
/// iteration awaits `make` afresh — useful when an async prep step (DB query,
/// fetch, decode) yields a finite collection to iterate over.
public func FromAsync<S: Sequence & Sendable>(
    _ make: @escaping @Sendable () async -> S,
) -> some PipeSource<S.Element, Never>
where S.Element: Sendable {
    AsyncDeferredSource<S, S.Element, Never>(
        make: make,
        makeNext: { inner in
            var iterator = inner.makeIterator()
            return { iterator.next().map(Result.success) }
        },
    )
}

/// Lift an async-produced `AsyncSequence` of `Result` elements into a pipeline source.
public func FromAsyncResult<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>(
    _ make: @escaping @Sendable () async -> S,
) -> some PipeSource<V, E>
where S.Element == Result<V, E>, S.Failure == Never {
    AsyncDeferredSource<S, V, E>(
        make: make,
        makeNext: { inner in
            var iterator = inner.makeAsyncIterator()
            return { await iterator.next(isolation: #isolation) }
        },
    )
}

/// Open-source marker — alias for `From(_:Input.Type)` provided for symmetry with the
/// closure-form `FromAsync`. Declares an `Input` type to be supplied at call time.
public func FromAsync<Input: Sendable>(_: Input.Type) -> OpenSource<Input> {
    OpenSource<Input>()
}

/// Open-source marker for a Result-bearing input — alias for `FromResult(_:_:)` provided
/// for symmetry with the closure-form `FromAsyncResult`.
public func FromAsyncResult<V: Sendable, E: Error & Sendable>(
    _: V.Type,
    _: E.Type,
) -> OpenResultSource<V, E> {
    OpenResultSource<V, E>()
}

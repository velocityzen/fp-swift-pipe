// MARK: - Source backing type

/// The source behind every `FromAsync` form. It awaits `make` when the first element is
/// needed, and it never reads ahead of the reader.
@usableFromInline
struct AsyncDeferredSource<Inner: Sendable, Output: Sendable, Failure: Error & Sendable>:
    PipeSource
{
    @usableFromInline let make: @Sendable () async -> Inner
    @usableFromInline let makeNext: @Sendable (Inner) -> () async -> Result<Output, Failure>?

    @inlinable
    init(
        make: @escaping @Sendable () async -> Inner,
        makeNext: @escaping @Sendable (Inner) -> () async -> Result<Output, Failure>?,
    ) {
        self.make = make
        self.makeNext = makeNext
    }

    @inlinable
    func produce() -> Pipe<Output, Failure> {
        let make = self.make
        let makeNext = self.makeNext
        return .iterating { .init(DeferredIterator(make: make, makeNext: makeNext)) }
    }
}

/// The iterator of `AsyncDeferredSource`. It calls `make` on the first `next()`.
@usableFromInline
struct DeferredIterator<Inner: Sendable, Element: Sendable>: AsyncIteratorProtocol {
    @usableFromInline
    enum State {
        case pending
        case running(() async -> Element?)
        case finished
    }

    @usableFromInline let make: @Sendable () async -> Inner
    @usableFromInline let makeNext: @Sendable (Inner) -> () async -> Element?
    @usableFromInline var state = State.pending

    @inlinable
    init(
        make: @escaping @Sendable () async -> Inner,
        makeNext: @escaping @Sendable (Inner) -> () async -> Element?,
    ) {
        self.make = make
        self.makeNext = makeNext
    }

    @inlinable
    mutating func next() async -> Element? {
        await next(isolation: #isolation)
    }

    @inlinable
    mutating func next(isolation actor: isolated (any Actor)?) async -> Element? {
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

// MARK: - DSL factories

/// Gives each element of an `AsyncSequence` that an async closure creates, as a success.
///
/// The closure runs on every run of the pipe, when the first element is needed. Use it for
/// async setup, for example to open a database cursor.
@inlinable
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

/// Gives each element of a `Sequence` that an async closure creates, as a success. The
/// closure runs when the first element is needed.
@inlinable
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

/// Gives each element of an `AsyncSequence` of `Result`s that an async closure creates. The
/// closure runs when the first element is needed.
@inlinable
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

/// Starts an open pipe whose input has the type `Input`. The same as `From(Input.self)`.
@inlinable
public func FromAsync<Input: Sendable>(_: Input.Type) -> OpenSource<Input> {
    OpenSource<Input>()
}

/// Starts an open pipe whose input is a sequence of `Result<V, E>`. The same as
/// `FromResult(V.self, E.self)`.
@inlinable
public func FromAsyncResult<V: Sendable, E: Error & Sendable>(
    _: V.Type,
    _: E.Type,
) -> OpenResultSource<V, E> {
    OpenResultSource<V, E>()
}

/// A source backed by an `AsyncSequence` whose elements are already `Result`s.
@usableFromInline
struct ResultAsyncSequenceSource<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>:
    PipeSource
where S.Element == Result<V, E>, S.Failure == Never {
    @usableFromInline typealias Output = V
    @usableFromInline typealias Failure = E

    @usableFromInline let make: @Sendable () -> S

    @inlinable
    init(_ make: @escaping @Sendable () -> S) {
        self.make = make
    }

    @inlinable
    func produce() -> Pipe<V, E> {
        let make = self.make
        return .iterating { .init(make().makeAsyncIterator()) }
    }
}

/// A source backed by an `AsyncSequence` of bare values; each value is lifted into `.success`
/// in a single iterator hop (no intermediate `AsyncMapSequence`).
@usableFromInline
struct LiftedAsyncSequenceSource<S: AsyncSequence & Sendable>: PipeSource
where S.Element: Sendable, S.Failure == Never {
    @usableFromInline typealias Output = S.Element
    @usableFromInline typealias Failure = Never

    @usableFromInline let make: @Sendable () -> S

    @inlinable
    init(_ make: @escaping @Sendable () -> S) {
        self.make = make
    }

    @inlinable
    func produce() -> Pipe<S.Element, Never> {
        let make = self.make
        return .iterating { .init(LiftingAsyncIterator(make().makeAsyncIterator())) }
    }
}

/// A source backed by a synchronous `Sequence`; each value is lifted into `.success`
/// in a single iterator hop (no `AsyncSyncSequence` + `AsyncMapSequence` double-bridging).
@usableFromInline
struct SyncSequenceSource<S: Sequence & Sendable>: PipeSource
where S.Element: Sendable {
    @usableFromInline typealias Output = S.Element
    @usableFromInline typealias Failure = Never

    @usableFromInline let make: @Sendable () -> S

    @inlinable
    init(_ make: @escaping @Sendable () -> S) {
        self.make = make
    }

    @inlinable
    func produce() -> Pipe<S.Element, Never> {
        let make = self.make
        return .iterating { .init(LiftingSyncIterator(make().makeIterator())) }
    }
}

// MARK: - DSL factories

/// Lift an `AsyncSequence` of bare values into a pipeline source (each value becomes `.success`).
///
/// `From(expr)` evaluates `expr` lazily on each iteration. The autoclosure captures the
/// *expression*, not a constructor — passing a stored reference to a single-shot
/// `AsyncSequence` (e.g. an `AsyncStream` you constructed once) will make the second
/// iteration of the pipe yield empty results without warning, because the underlying
/// iterator has already been consumed. For non-replayable sources, use `Defer { … }`,
/// which re-invokes the closure per iteration:
///
/// ```swift
/// // Wrong — second iteration yields empty, no diagnostic.
/// let stream = AsyncStream<Int> { … }
/// let pipe = Pipe { From(stream); Map { … } }
///
/// // Right — Defer constructs a fresh stream per iteration.
/// let pipe = Pipe { Defer { AsyncStream<Int> { … } }; Map { … } }
/// ```
///
/// `Array`, `Range`, and other replayable sources work fine with `From`.
@inlinable
public func From<S: AsyncSequence & Sendable>(
    _ source: @autoclosure @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable, S.Failure == Never {
    LiftedAsyncSequenceSource(source)
}

/// Lift an `AsyncSequence` of `Result` elements into a pipeline source.
@inlinable
public func FromResult<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>(
    _ source: @autoclosure @escaping @Sendable () -> S,
) -> some PipeSource<V, E> where S.Element == Result<V, E>, S.Failure == Never {
    ResultAsyncSequenceSource(source)
}

/// Lift a synchronous `Sequence` into a pipeline source.
@inlinable
public func From<S: Sequence & Sendable>(
    _ source: @autoclosure @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable {
    SyncSequenceSource(source)
}

/// Open-source marker — declares an `Input` type to be supplied at call time. The enclosing
/// `OpenPipe { … }` builder produces an `OpenPipe<Input, …, …>` that's callable as
/// `pipe(source)`.
@inlinable
public func From<Input: Sendable>(_: Input.Type) -> OpenSource<Input> {
    OpenSource<Input>()
}

/// Open-source marker for a Result-bearing input — declares an inner value/failure type
/// to be supplied at call time as an `AsyncSequence<Result<V, E>>` (or `Sequence`). The
/// enclosing `OpenPipe { … }` builder produces an `OpenPipe<Result<V, E>, V, E>` whose
/// downstream stages see unwrapped successes and failures lifted into the channel.
@inlinable
public func FromResult<V: Sendable, E: Error & Sendable>(
    _: V.Type,
    _: E.Type,
) -> OpenResultSource<V, E> {
    OpenResultSource<V, E>()
}

// MARK: - Deferred sources (non-autoclosure form)

/// Defer construction of an `AsyncSequence` source until the pipeline is iterated.
/// Each iteration invokes the closure afresh — useful for sources that aren't
/// re-iterable (e.g. an `AsyncStream` you build from a fresh continuation).
@inlinable
public func Defer<S: AsyncSequence & Sendable>(
    _ make: @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable, S.Failure == Never {
    LiftedAsyncSequenceSource(make)
}

/// Deferred form of `FromResult` for `Result`-bearing sequences.
@inlinable
public func DeferResult<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>(
    _ make: @escaping @Sendable () -> S,
) -> some PipeSource<V, E> where S.Element == Result<V, E>, S.Failure == Never {
    ResultAsyncSequenceSource(make)
}

/// Deferred form of `From` for synchronous sequences.
@inlinable
public func Defer<S: Sequence & Sendable>(
    _ make: @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable {
    SyncSequenceSource(make)
}

// MARK: - Internal lifting bridges

/// Reads a synchronous `Sequence` and yields `Result.success(element)` per element.
@usableFromInline
struct LiftingSyncIterator<Base: IteratorProtocol>: AsyncIteratorProtocol {
    @usableFromInline var base: Base

    @inlinable
    init(_ base: Base) {
        self.base = base
    }

    @inlinable
    mutating func next() async -> Result<Base.Element, Never>? {
        base.next().map(Result.success)
    }

    @inlinable
    mutating func next(isolation actor: isolated (any Actor)?) async -> Result<Base.Element, Never>?
    {
        base.next().map(Result.success)
    }
}

/// Reads an `AsyncSequence` and yields `Result.success(element)` per element.
@usableFromInline
struct LiftingAsyncIterator<Base: AsyncIteratorProtocol>: AsyncIteratorProtocol
where Base.Failure == Never {
    @usableFromInline var base: Base

    @inlinable
    init(_ base: Base) {
        self.base = base
    }

    @inlinable
    mutating func next() async -> Result<Base.Element, Never>? {
        await next(isolation: #isolation)
    }

    @inlinable
    mutating func next(isolation actor: isolated (any Actor)?) async -> Result<Base.Element, Never>?
    {
        // Stays on the caller's isolation; `Base.Failure == Never` makes it non-throwing.
        guard let element = await base.next(isolation: actor) else { return nil }
        return .success(element)
    }
}

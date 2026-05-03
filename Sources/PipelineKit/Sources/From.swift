/// A source backed by an `AsyncSequence` whose elements are already `Result`s.
struct ResultAsyncSequenceSource<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>:
    PipeSource
where S.Element == Result<V, E> {
    typealias Output = V
    typealias Failure = E

    private let make: @Sendable () -> S

    init(_ make: @escaping @Sendable () -> S) {
        self.make = make
    }

    func produce() -> Pipe<V, E> {
        let make = self.make
        return .erased { AnyAsyncSequence(make()) }
    }
}

/// A source backed by an `AsyncSequence` of bare values; each value is lifted into `.success`
/// in a single iterator hop (no intermediate `AsyncMapSequence`).
struct LiftedAsyncSequenceSource<S: AsyncSequence & Sendable>: PipeSource
where S.Element: Sendable {
    typealias Output = S.Element
    typealias Failure = Never

    private let make: @Sendable () -> S

    init(_ make: @escaping @Sendable () -> S) {
        self.make = make
    }

    func produce() -> Pipe<S.Element, Never> {
        let make = self.make
        return .erased { AnyAsyncSequence(LiftingAsyncSequence(make())) }
    }
}

/// A source backed by a synchronous `Sequence`; each value is lifted into `.success`
/// in a single iterator hop (no `AsyncSyncSequence` + `AsyncMapSequence` double-bridging).
struct SyncSequenceSource<S: Sequence & Sendable>: PipeSource
where S.Element: Sendable {
    typealias Output = S.Element
    typealias Failure = Never

    private let make: @Sendable () -> S

    init(_ make: @escaping @Sendable () -> S) {
        self.make = make
    }

    func produce() -> Pipe<S.Element, Never> {
        let make = self.make
        return .erased { AnyAsyncSequence(LiftingSyncSequence(make())) }
    }
}

// MARK: - DSL factories

/// Lift an `AsyncSequence` of bare values into a pipeline source (each value becomes `.success`).
///
/// `From(expr)` evaluates `expr` lazily on each iteration; for multi-statement construction
/// use `Defer { … }` instead.
public func From<S: AsyncSequence & Sendable>(
    _ source: @autoclosure @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable {
    LiftedAsyncSequenceSource(source)
}

/// Lift an `AsyncSequence` of `Result` elements into a pipeline source.
public func FromResult<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>(
    _ source: @autoclosure @escaping @Sendable () -> S,
) -> some PipeSource<V, E> where S.Element == Result<V, E> {
    ResultAsyncSequenceSource(source)
}

/// Lift a synchronous `Sequence` into a pipeline source.
public func From<S: Sequence & Sendable>(
    _ source: @autoclosure @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable {
    SyncSequenceSource(source)
}

/// Open-source marker — declares an `Input` type to be supplied at call time. The enclosing
/// `OpenPipe { … }` builder produces an `OpenPipe<Input, …, …>` that's callable as
/// `pipe(source)`.
public func From<Input: Sendable>(_: Input.Type) -> OpenSource<Input> {
    OpenSource<Input>()
}

/// Open-source marker for a Result-bearing input — declares an inner value/failure type
/// to be supplied at call time as an `AsyncSequence<Result<V, E>>` (or `Sequence`). The
/// enclosing `OpenPipe { … }` builder produces an `OpenPipe<Result<V, E>, V, E>` whose
/// downstream stages see unwrapped successes and failures lifted into the channel.
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
public func Defer<S: AsyncSequence & Sendable>(
    _ make: @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable {
    LiftedAsyncSequenceSource(make)
}

/// Deferred form of `FromResult` for `Result`-bearing sequences.
public func DeferResult<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>(
    _ make: @escaping @Sendable () -> S,
) -> some PipeSource<V, E> where S.Element == Result<V, E> {
    ResultAsyncSequenceSource(make)
}

/// Deferred form of `From` for synchronous sequences.
public func Defer<S: Sequence & Sendable>(
    _ make: @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable {
    SyncSequenceSource(make)
}

// MARK: - Internal lifting bridges

/// Wraps a synchronous `Sequence` and yields `Result.success(element)` per iteration.
/// One iterator hop instead of `AsyncSyncSequence + AsyncMapSequence` (~150 ns/elem saving
/// in debug; smaller in release).
struct LiftingSyncSequence<Base: Sequence & Sendable>: AsyncSequence
where Base.Element: Sendable {
    typealias Element = Result<Base.Element, Never>

    let base: Base
    init(_ base: Base) {
        self.base = base
    }

    func makeAsyncIterator() -> Iterator {
        Iterator(base.makeIterator())
    }

    struct Iterator: AsyncIteratorProtocol {
        var iterator: Base.Iterator
        init(_ iterator: Base.Iterator) {
            self.iterator = iterator
        }

        mutating func next() async -> Result<Base.Element, Never>? {
            iterator.next().map(Result.success)
        }
    }
}

/// Wraps an `AsyncSequence` and yields `Result.success(element)` per iteration, in a
/// single hop instead of going through `AsyncMapSequence`.
struct LiftingAsyncSequence<Base: AsyncSequence & Sendable>: AsyncSequence
where Base.Element: Sendable {
    typealias Element = Result<Base.Element, Never>

    let base: Base
    init(_ base: Base) {
        self.base = base
    }

    func makeAsyncIterator() -> Iterator {
        Iterator(base.makeAsyncIterator())
    }

    struct Iterator: AsyncIteratorProtocol {
        var iterator: Base.AsyncIterator
        init(_ iterator: Base.AsyncIterator) {
            self.iterator = iterator
        }

        mutating func next() async -> Result<Base.Element, Never>? {
            // V0.1 contract: only non-throwing upstreams reach here. Coerce a hypothetical
            // throw to end-of-sequence to preserve the surface API.
            guard let element = try? await iterator.next() else { return nil }
            return .success(element)
        }
    }
}

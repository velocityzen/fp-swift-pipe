/// A source that reads an `AsyncSequence` of `Result`s and gives each element unchanged.
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

/// A source that reads an `AsyncSequence` and gives each element as a success.
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

/// A source that reads a `Sequence` and gives each element as a success. It reads the
/// sequence directly, without first turning it into an `AsyncSequence`.
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

/// Gives each element of an `AsyncSequence` as a success.
///
/// A stored sequence that can be read only once, like an `AsyncStream`, is empty on the second
/// run of the pipe, and there is no error. Create it inside `Defer` instead:
///
/// ```swift
/// Defer { makeStream() }   // a new stream on every run
/// ```
@inlinable
public func From<S: AsyncSequence & Sendable>(
    _ source: @autoclosure @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable, S.Failure == Never {
    LiftedAsyncSequenceSource(source)
}

/// Gives each element of an `AsyncSequence` of `Result`s, unchanged.
///
/// A pipe can read another pipe this way. For a sequence that can be read only once, use
/// `DeferResult`.
@inlinable
public func FromResult<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>(
    _ source: @autoclosure @escaping @Sendable () -> S,
) -> some PipeSource<V, E> where S.Element == Result<V, E>, S.Failure == Never {
    ResultAsyncSequenceSource(source)
}

/// Gives each element of a `Sequence` as a success.
@inlinable
public func From<S: Sequence & Sendable>(
    _ source: @autoclosure @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable {
    SyncSequenceSource(source)
}

/// Starts an open pipe whose input has the type `Input`. See <doc:OpenPipes>.
@inlinable
public func From<Input: Sendable>(_: Input.Type) -> OpenSource<Input> {
    OpenSource<Input>()
}

/// Starts an open pipe whose input is a sequence of `Result<V, E>`. See <doc:OpenPipes>.
@inlinable
public func FromResult<V: Sendable, E: Error & Sendable>(
    _: V.Type,
    _: E.Type,
) -> OpenResultSource<V, E> {
    OpenResultSource<V, E>()
}

// MARK: - Deferred sources (non-autoclosure form)

/// Gives each element of an `AsyncSequence` that the closure creates, as a success. The
/// closure runs again on every run of the pipe.
@inlinable
public func Defer<S: AsyncSequence & Sendable>(
    _ make: @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable, S.Failure == Never {
    LiftedAsyncSequenceSource(make)
}

/// Gives each element of an `AsyncSequence` of `Result`s that the closure creates. The
/// closure runs again on every run of the pipe.
@inlinable
public func DeferResult<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>(
    _ make: @escaping @Sendable () -> S,
) -> some PipeSource<V, E> where S.Element == Result<V, E>, S.Failure == Never {
    ResultAsyncSequenceSource(make)
}

/// Gives each element of a `Sequence` that the closure creates, as a success. The closure
/// runs again on every run of the pipe.
@inlinable
public func Defer<S: Sequence & Sendable>(
    _ make: @escaping @Sendable () -> S,
) -> some PipeSource<S.Element, Never> where S.Element: Sendable {
    SyncSequenceSource(make)
}

// MARK: - Internal lifting bridges

/// Reads a `Sequence` and gives each element as a success.
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

/// Reads an `AsyncSequence` and gives each element as a success.
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
        // Stays on the caller's actor. It cannot throw (`Base.Failure == Never`).
        guard let element = await base.next(isolation: actor) else { return nil }
        return .success(element)
    }
}

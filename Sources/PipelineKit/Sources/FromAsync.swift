import FP

// MARK: - Source backing types

/// Defers an `AsyncSequence` source behind an `async` closure. Each iteration awaits
/// the closure, then drains the returned sequence.
struct AsyncDeferredAsyncSequenceSource<S: AsyncSequence & Sendable>: PipeSource
where S.Element: Sendable, S.Failure == Never {
    typealias Output = S.Element
    typealias Failure = Never

    private let make: @Sendable () async -> S

    init(_ make: @escaping @Sendable () async -> S) {
        self.make = make
    }

    func produce() -> Pipe<S.Element, Never> {
        let make = self.make
        return .erased {
            AnyAsyncSequence(
                // `.unbounded` is explicit. AsyncStream has no native backpressure; if the
                // produced sequence outpaces a stalled consumer, the queue grows. For
                // typical async sources (cursors, network streams) production is naturally
                // throttled by the upstream's own latency.
                AsyncStream<Result<S.Element, Never>>(bufferingPolicy: .unbounded) {
                    continuation in
                    let task = Task {
                        let seq = await make()
                        for await element in seq {
                            continuation.success(element)
                            if Task.isCancelled { break }
                        }
                        continuation.finish()
                    }
                    continuation.onTermination = { _ in task.cancel() }
                },
            )
        }
    }
}

/// Defers a synchronous `Sequence` source behind an `async` closure. Each iteration
/// awaits the closure, then drains the returned sequence.
struct AsyncDeferredSyncSequenceSource<S: Sequence & Sendable>: PipeSource
where S.Element: Sendable {
    typealias Output = S.Element
    typealias Failure = Never

    private let make: @Sendable () async -> S

    init(_ make: @escaping @Sendable () async -> S) {
        self.make = make
    }

    func produce() -> Pipe<S.Element, Never> {
        let make = self.make
        return .erased {
            AnyAsyncSequence(
                // `.unbounded` is explicit. The sync sequence is drained as fast as the
                // task can run; if the consumer stalls and the sequence is large, the
                // queue grows without bound.
                AsyncStream<Result<S.Element, Never>>(bufferingPolicy: .unbounded) {
                    continuation in
                    let task = Task {
                        let seq = await make()
                        for element in seq {
                            continuation.success(element)
                            if Task.isCancelled { break }
                        }
                        continuation.finish()
                    }
                    continuation.onTermination = { _ in task.cancel() }
                },
            )
        }
    }
}

/// Defers a `Result`-bearing `AsyncSequence` source behind an `async` closure.
struct AsyncDeferredResultSequenceSource<
    S: AsyncSequence & Sendable,
    V: Sendable,
    E: Error & Sendable,
>: PipeSource
where S.Element == Result<V, E>, S.Failure == Never {
    typealias Output = V
    typealias Failure = E

    private let make: @Sendable () async -> S

    init(_ make: @escaping @Sendable () async -> S) {
        self.make = make
    }

    func produce() -> Pipe<V, E> {
        let make = self.make
        return .erased {
            AnyAsyncSequence(
                // `.unbounded` is explicit; see sibling sources for the backpressure note.
                AsyncStream<Result<V, E>>(bufferingPolicy: .unbounded) { continuation in
                    let task = Task {
                        let seq = await make()
                        for await element in seq {
                            continuation.yield(element)
                            if Task.isCancelled { break }
                        }
                        continuation.finish()
                    }
                    continuation.onTermination = { _ in task.cancel() }
                },
            )
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
    AsyncDeferredAsyncSequenceSource(make)
}

/// Lift an async-produced synchronous `Sequence` into a pipeline source. Each
/// iteration awaits `make` afresh — useful when an async prep step (DB query,
/// fetch, decode) yields a finite collection to iterate over.
public func FromAsync<S: Sequence & Sendable>(
    _ make: @escaping @Sendable () async -> S,
) -> some PipeSource<S.Element, Never>
where S.Element: Sendable {
    AsyncDeferredSyncSequenceSource(make)
}

/// Lift an async-produced `AsyncSequence` of `Result` elements into a pipeline source.
public func FromAsyncResult<S: AsyncSequence & Sendable, V: Sendable, E: Error & Sendable>(
    _ make: @escaping @Sendable () async -> S,
) -> some PipeSource<V, E>
where S.Element == Result<V, E>, S.Failure == Never {
    AsyncDeferredResultSequenceSource(make)
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

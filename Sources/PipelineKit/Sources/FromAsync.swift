import FP

// MARK: - Source backing type

/// Defers a source behind an `async` constructor. On each iteration, `make` is awaited to
/// produce an inner sequence, which is then drained via the supplied `drain` closure. The
/// three differing source shapes (async/sync iteration × bare/Result element) all collapse
/// into this single struct — the variation lives entirely in the `drain` closure.
struct AsyncDeferredSource<Inner: Sendable, Output: Sendable, Failure: Error & Sendable>:
    PipeSource
{
    private let make: @Sendable () async -> Inner
    private let drain:
        @Sendable (Inner, AsyncStream<Result<Output, Failure>>.Continuation) async -> Void

    init(
        make: @escaping @Sendable () async -> Inner,
        drain:
            @escaping @Sendable (Inner, AsyncStream<Result<Output, Failure>>.Continuation)
            async -> Void,
    ) {
        self.make = make
        self.drain = drain
    }

    func produce() -> Pipe<Output, Failure> {
        let make = self.make
        let drain = self.drain
        return .erased {
            AnyAsyncSequence(
                // `.unbounded` is explicit. AsyncStream has no native backpressure; if the
                // produced sequence outpaces a stalled consumer, the queue grows. For typical
                // async sources (cursors, network streams) production is throttled by the
                // upstream's own latency.
                AsyncStream<Result<Output, Failure>>(bufferingPolicy: .unbounded) { continuation in
                    let task = Task {
                        let inner = await make()
                        await drain(inner, continuation)
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
    AsyncDeferredSource<S, S.Element, Never>(
        make: make,
        drain: { inner, continuation in
            for await element in inner {
                continuation.success(element)
                if Task.isCancelled { break }
            }
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
        drain: { inner, continuation in
            for element in inner {
                continuation.success(element)
                if Task.isCancelled { break }
            }
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
        drain: { inner, continuation in
            for await element in inner {
                continuation.yield(element)
                if Task.isCancelled { break }
            }
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

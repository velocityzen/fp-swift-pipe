/// A re-callable pipeline whose source is supplied at call time.
///
/// `OpenPipe` is what you get when a pipeline starts with a `From(Input.self)`
/// (or `FromAsync(Input.self)`) marker instead of a concrete source. The marker leaves
/// a hole at the source position; calling the open pipe with a real `Sequence` or
/// `AsyncSequence` plugs that hole and returns a regular `Pipe<Success, Failure>`.
///
/// ```swift
/// let pipe = OpenPipe {
///     From(Int.self)
///     Map { (n: Int) in n * 10 }
///     Filter { $0 > 50 }
/// }
/// for await x in pipe([1, 5, 10, 20]) { … }
/// ```
public struct OpenPipe<Input: Sendable, Success: Sendable, Failure: Error & Sendable>: Sendable {
    /// Apply the accumulated stages to a starter pipe — set by `PipeBuilder`.
    let apply: @Sendable (Pipe<Input, Never>) -> Pipe<Success, Failure>

    /// Apply the open pipe to an `AsyncSequence`, returning a regular `Pipe` ready to iterate.
    public func callAsFunction<S: AsyncSequence & Sendable>(
        _ source: S,
    ) -> Pipe<Success, Failure> where S.Element == Input, S.Failure == Never {
        apply(Pipe { From(source) })
    }

    /// Apply the open pipe to a synchronous `Sequence`, returning a regular `Pipe`.
    public func callAsFunction<S: Sequence & Sendable>(
        _ source: S,
    ) -> Pipe<Success, Failure> where S.Element == Input {
        apply(Pipe { From(source) })
    }
}

public extension OpenPipe {
    /// Build an open pipeline from a `From<Input>()` marker followed by stages.
    init(@PipeBuilder _ build: () -> OpenPipe<Input, Success, Failure>) {
        self = build()
    }
}

/// Marker source for an "open" pipeline — a hole at the source position that
/// `OpenPipe.callAsFunction` later fills with a real `Sequence` / `AsyncSequence`.
public struct OpenSource<Input: Sendable>: Sendable {
    public init() {}
}

/// Marker source for an "open" Result-bearing pipeline — declares the value/failure types
/// of an `AsyncSequence<Result<V, E>>` (or `Sequence<Result<V, E>>`) to be supplied at
/// call time. Unlike `OpenSource`, the inner `Result`s lift directly into the pipe's
/// failure channel — `pipe(stream)` returns `Pipe<…, E>`, and stages downstream see
/// the unwrapped successes / propagated failures.
public struct OpenResultSource<Value: Sendable, Failure: Error & Sendable>: Sendable {
    public init() {}
}

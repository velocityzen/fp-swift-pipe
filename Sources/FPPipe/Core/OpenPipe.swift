/// A pipe that has no input yet.
///
/// Start it with `From(T.self)`. Call it with a sequence to get a `Pipe`. See <doc:OpenPipes>.
///
/// ```swift
/// let double = OpenPipe {
///     From(Int.self)
///     Filter { (n: Int) in n > 0 }
///     Map { (n: Int) in n * 2 }
/// }
///
/// await double([1, -1, 2]).toResult()   // .success([2, 4])
/// ```
public struct OpenPipe<Input: Sendable, Success: Sendable, Failure: Error & Sendable>: Sendable {
    /// Runs the stages on a pipe made from the input. `PipeBuilder` sets it.
    @usableFromInline let apply: @Sendable (Pipe<Input, Never>) -> Pipe<Success, Failure>

    @inlinable
    init(apply: @escaping @Sendable (Pipe<Input, Never>) -> Pipe<Success, Failure>) {
        self.apply = apply
    }

    /// Returns a pipe that reads this `AsyncSequence` as its source.
    @inlinable
    public func callAsFunction<S: AsyncSequence & Sendable>(
        _ source: S,
    ) -> Pipe<Success, Failure> where S.Element == Input, S.Failure == Never {
        apply(Pipe { From(source) })
    }

    /// Returns a pipe that reads this `Sequence` as its source.
    @inlinable
    public func callAsFunction<S: Sequence & Sendable>(
        _ source: S,
    ) -> Pipe<Success, Failure> where S.Element == Input {
        apply(Pipe { From(source) })
    }
}

public extension OpenPipe {
    /// Creates an open pipe. The first line must be like `From(T.self)`.
    @inlinable
    init(@PipeBuilder _ build: () -> OpenPipe<Input, Success, Failure>) {
        self = build()
    }
}

/// The source of an open pipe, made by `From(T.self)`. It has no input yet.
public struct OpenSource<Input: Sendable>: Sendable {
    @inlinable
    public init() {}
}

/// The source of an open pipe that reads `Result`s, made by `FromResult(V.self, E.self)`.
///
/// Failures in the input go straight into the failure channel.
public struct OpenResultSource<Value: Sendable, Failure: Error & Sendable>: Sendable {
    @inlinable
    public init() {}
}

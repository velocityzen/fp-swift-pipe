/// An `AsyncSequence` of `Result<Success, Failure>`, built from a source and stages.
///
/// A pipe runs each time you read it, with a sink or `for await`. A source that can be read
/// only once, like a stored `AsyncStream`, is empty on the second run. See
/// <doc:GettingStarted>.
public struct Pipe<Success: Sendable, Failure: Error & Sendable>: AsyncSequence, Sendable {
    public typealias Element = Result<Success, Failure>
    public typealias AsyncIterator = AnyAsyncSequence<Element>.Iterator

    /// Builds the iterators for one run. Sources and stages set it.
    @usableFromInline let base: AnyAsyncSequence<Element>

    @inlinable
    init(base: AnyAsyncSequence<Element>) {
        self.base = base
    }

    /// Returns a pipe that calls `makeIterator` on every run. It is not an `init`, so
    /// `Pipe { … }` always means the builder.
    @inlinable
    static func iterating(_ makeIterator: @escaping @Sendable () -> AsyncIterator) -> Pipe {
        Pipe(base: AnyAsyncSequence(makeIterator: makeIterator))
    }

    @inlinable
    public func makeAsyncIterator() -> AsyncIterator {
        base.makeAsyncIterator()
    }
}

extension Pipe where Failure == Never {
    /// Changes the failure type from `Never` to `F`. The elements stay the same.
    @inlinable
    func widenFailure<F: Error & Sendable>(to _: F.Type) -> Pipe<Success, F> {
        Pipe<Success, F>.iterating {
            .init(StepIterator(makeAsyncIterator(), step: WidenFailureStep<Success, F>()))
        }
    }
}

/// Gives a `Result` that cannot fail a new failure type.
@usableFromInline
struct WidenFailureStep<Value: Sendable, F: Error & Sendable>: PipeStep {
    @inlinable
    init() {}

    @inlinable
    func callAsFunction(_ element: Result<Value, Never>) -> Result<Value, F>? {
        switch element {
            case .success(let value): .success(value)
        }
    }
}

/// A lazy, re-iterable description of an async stream of `Result<Success, Failure>`.
///
/// `Pipe` is itself an `AsyncSequence`. Each iteration reconstructs the underlying
/// chain of iterators via the stored closure, so pipelines compose without sharing
/// state and can be iterated more than once when their sources permit.
///
/// Sources and stages are inlinable and build their iterators from concrete types, so a
/// stage's work compiles specialized into the module that builds the pipeline; the only
/// indirection left is one call per element from each stage to the one above it.
public struct Pipe<Success: Sendable, Failure: Error & Sendable>: AsyncSequence, Sendable {
    public typealias Element = Result<Success, Failure>
    public typealias AsyncIterator = AnyAsyncSequence<Element>.Iterator

    /// Builds the chain of iterators for one iteration — set by sources and stages.
    @usableFromInline let base: AnyAsyncSequence<Element>

    @inlinable
    init(base: AnyAsyncSequence<Element>) {
        self.base = base
    }

    /// The pipe each source and stage returns: every iteration runs the iterator
    /// `makeIterator` builds. A static function rather than an `init` taking a
    /// closure, so `Pipe { … }` always means the builder, `@testable import` or not.
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
    /// Widen a non-failable pipeline into any concrete failure channel. Cheap — the
    /// transform is provably unreachable for `.failure`, so each element is just
    /// re-wrapped on the success branch.
    @inlinable
    func widenFailure<F: Error & Sendable>(to _: F.Type) -> Pipe<Success, F> {
        Pipe<Success, F>.iterating {
            .init(StepIterator(makeAsyncIterator(), step: WidenFailureStep<Success, F>()))
        }
    }
}

/// Re-types a `Result` that cannot fail under a concrete failure type.
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

/// A lazy, re-iterable description of an async stream of `Result<Success, Failure>`.
///
/// `Pipe` is itself an `AsyncSequence`. Each iteration reconstructs the underlying
/// chain via the stored builder closure, so pipelines compose without sharing state
/// and can be iterated more than once when their sources permit.
public struct Pipe<Success: Sendable, Failure: Error & Sendable>: AsyncSequence, Sendable {
    public typealias Element = Result<Success, Failure>
    public typealias AsyncIterator = AnyAsyncSequence<Element>.Iterator

    private let _build: @Sendable () -> AnyAsyncSequence<Element>

    /// Internal factory — only used by sources/stages within the package.
    static func erased(_ build: @escaping @Sendable () -> AnyAsyncSequence<Element>) -> Pipe {
        Pipe(_build: build)
    }

    public func makeAsyncIterator() -> AsyncIterator {
        _build().makeAsyncIterator()
    }

    /// Internal access to the type-erased upstream — used by stages to attach.
    func upstream() -> AnyAsyncSequence<Element> {
        _build()
    }
}

extension Pipe where Failure == Never {
    /// Widen a non-failable pipeline into any concrete failure channel. Cheap — the
    /// transform is provably unreachable for `.failure`, so each element is just
    /// re-wrapped on the success branch.
    func widenFailure<F: Error & Sendable>(to _: F.Type) -> Pipe<Success, F> {
        let upstream = self.upstream()
        return .erased {
            AnyAsyncSequence(
                upstream.map { (element: Result<Success, Never>) -> Result<Success, F> in
                    switch element {
                        case .success(let value): return .success(value)
                    }
                },
            )
        }
    }
}

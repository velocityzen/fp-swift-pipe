/// A source that gives one failure and then finishes.
@usableFromInline
struct FailureSource<V: Sendable, F: Error & Sendable>: PipeSource {
    @usableFromInline typealias Output = V
    @usableFromInline typealias Failure = F

    @usableFromInline let error: F

    @inlinable
    init(_ error: F) {
        self.error = error
    }

    @inlinable
    func produce() -> Pipe<V, F> {
        let error = self.error
        return .iterating { .init(OneElementIterator<Result<V, F>>(.failure(error))) }
    }
}

/// Gives one failure and then finishes.
@inlinable
public func Failure<V: Sendable, F: Error & Sendable>(
    _ error: F,
    valueType _: V.Type,
) -> some PipeSource<V, F> {
    FailureSource(error)
}

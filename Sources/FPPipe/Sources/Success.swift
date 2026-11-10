/// A source that gives one success and then finishes. It cannot fail.
@usableFromInline
struct SuccessSource<V: Sendable>: PipeSource {
    @usableFromInline typealias Output = V
    @usableFromInline typealias Failure = Never

    @usableFromInline let value: V

    @inlinable
    init(_ value: V) {
        self.value = value
    }

    @inlinable
    func produce() -> Pipe<V, Never> {
        let value = self.value
        return .iterating { .init(OneElementIterator<Result<V, Never>>(.success(value))) }
    }
}

/// Gives one success and then finishes. The source cannot fail.
@inlinable
public func Success<V: Sendable>(_ value: V) -> some PipeSource<V, Never> {
    SuccessSource(value)
}

/// Gives one success and then finishes. The same as `Success(_:)`.
@inlinable
public func Of<V: Sendable>(_ value: V) -> some PipeSource<V, Never> {
    SuccessSource(value)
}

/// A source that emits a single `.success(value)` and finishes. Cannot fail.
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

/// DSL: `Success(42)` — a one-shot source that always succeeds.
@inlinable
public func Success<V: Sendable>(_ value: V) -> some PipeSource<V, Never> {
    SuccessSource(value)
}

/// DSL alias: `Of(42)` — equivalent to `Success(value)`. Familiar to readers
/// from `Applicative.of` / `pure` in fp-swift's adjacent libraries.
@inlinable
public func Of<V: Sendable>(_ value: V) -> some PipeSource<V, Never> {
    SuccessSource(value)
}

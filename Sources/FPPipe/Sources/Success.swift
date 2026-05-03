import FP

/// A source that emits a single `.success(value)` and finishes. Cannot fail.
struct SuccessSource<V: Sendable>: PipeSource {
    typealias Output = V
    typealias Failure = Never

    private let value: V

    init(_ value: V) {
        self.value = value
    }

    func produce() -> Pipe<V, Never> {
        let value = self.value
        return .erased {
            AnyAsyncSequence(AsyncStream<Result<V, Never>>.success(value))
        }
    }
}

/// DSL: `Success(42)` — a one-shot source that always succeeds.
public func Success<V: Sendable>(_ value: V) -> some PipeSource<V, Never> {
    SuccessSource(value)
}

/// DSL alias: `Of(42)` — equivalent to `Success(value)`. Familiar to readers
/// from `Applicative.of` / `pure` in fp-swift's adjacent libraries.
public func Of<V: Sendable>(_ value: V) -> some PipeSource<V, Never> {
    SuccessSource(value)
}

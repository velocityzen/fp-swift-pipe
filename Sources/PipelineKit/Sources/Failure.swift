import FP

/// A source that emits a single `.failure` and finishes. The success type must
/// be supplied because no value is ever produced.
struct FailureSource<V: Sendable, F: Error & Sendable>: PipeSource {
    typealias Output = V
    typealias Failure = F

    private let error: F

    init(_ error: F) {
        self.error = error
    }

    func produce() -> Pipe<V, F> {
        let error = self.error
        return .erased {
            AnyAsyncSequence(AsyncStream<Result<V, F>>.failure(error))
        }
    }
}

/// DSL: `Failure(AppError.bad, valueType: Int.self)` — a one-shot source that always fails.
/// The value type cannot be inferred (no value is produced) so it must be supplied.
public func Failure<V: Sendable, F: Error & Sendable>(
    _ error: F,
    valueType _: V.Type,
) -> some PipeSource<V, F> {
    FailureSource(error)
}

/// A source that finishes immediately with no elements. Both type parameters must
/// be supplied at the call site since no value is ever produced.
struct EmptySource<V: Sendable, F: Error & Sendable>: PipeSource {
    typealias Output = V
    typealias Failure = F

    func produce() -> Pipe<V, F> {
        .erased {
            AnyAsyncSequence(AsyncStream<Result<V, F>> { $0.finish() })
        }
    }
}

/// DSL: `Empty(valueType: Int.self, failureType: AppError.self)` — a source that finishes
/// without emitting. Both type parameters must be supplied since no value is ever produced.
public func Empty<V: Sendable, F: Error & Sendable>(
    valueType _: V.Type,
    failureType _: F.Type,
) -> some PipeSource<V, F> {
    EmptySource()
}

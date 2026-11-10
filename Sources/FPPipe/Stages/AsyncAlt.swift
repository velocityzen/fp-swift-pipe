import FP

/// The stage that `AsyncAlt` returns.
@usableFromInline
struct AsyncAltStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

    @usableFromInline let alternative: @Sendable () async -> Result<Value, F>
    @usableFromInline let concurrency: Int
    @usableFromInline let ordered: Bool
    @usableFromInline let bufferLimit: Int

    @inlinable
    init(
        _ alternative: @escaping @Sendable () async -> Result<Value, F>,
        concurrency: Int,
        ordered: Bool,
        bufferLimit: Int,
    ) {
        self.alternative = alternative
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
        self.ordered = ordered
        precondition(bufferLimit >= 0, "bufferLimit must be at least 0")
        self.bufferLimit = bufferLimit
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = AsyncAltStep(alternative)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        let ordered = self.ordered
        let bufferLimit = self.bufferLimit
        return .iterating {
            upstream.compactMapAsyncIterator(
                concurrency: concurrency,
                ordered: ordered,
                bufferLimit: bufferLimit,
                passthrough: { $0.passthroughSuccess() },
                { element in await step(element) },
            )
        }
    }
}

@usableFromInline
struct AsyncAltStep<Value, F: Error>: AsyncPipeStep {
    @usableFromInline let alternative: @Sendable () async -> Result<Value, F>

    @inlinable
    init(_ alternative: @escaping @Sendable () async -> Result<Value, F>) {
        self.alternative = alternative
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) async -> Result<Value, F>? {
        await element.altAsync(alternative)
    }
}

/// Replaces each failure with the `Result` that an async closure returns.
///
/// The closure does not see the error. Successes pass through unchanged. `concurrency`,
/// `ordered` and `bufferLimit` work as in `AsyncMap`.
@inlinable
public func AsyncAlt<Value: Sendable, F: Error & Sendable>(
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    _ alternative: @escaping @Sendable () async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncAltStage(alternative, concurrency: concurrency, ordered: ordered, bufferLimit: bufferLimit)
}

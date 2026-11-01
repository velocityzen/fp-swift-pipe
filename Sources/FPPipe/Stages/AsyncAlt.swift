import FP

/// Asynchronously replace each `.failure` with an alternative `Result` that doesn't
/// see the error. Mirrors fp-swift's `Result.altAsync`.
///
/// `concurrency` controls how many fallbacks run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered).
@usableFromInline
struct AsyncAltStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

    @usableFromInline let alternative: @Sendable () async -> Result<Value, F>
    @usableFromInline let concurrency: Int

    @inlinable
    init(
        _ alternative: @escaping @Sendable () async -> Result<Value, F>,
        concurrency: Int,
    ) {
        self.alternative = alternative
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = AsyncAltStep(alternative)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        return .iterating {
            .init(
                upstream.compactMapAsyncUnordered(
                    concurrency: concurrency,
                    passthrough: { $0.passthroughSuccess() },
                    { element in await step(element) },
                )
                .makeAsyncIterator(),
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

/// DSL: `AsyncAlt { await fetchFromCache() }`.
@inlinable
public func AsyncAlt<Value: Sendable, F: Error & Sendable>(
    concurrency: Int = 1,
    _ alternative: @escaping @Sendable () async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncAltStage(alternative, concurrency: concurrency)
}

import FP

/// Asynchronously replace each `.failure` with a `Success` derived from the error.
/// The output pipeline cannot fail (`Failure == Never`). Mirrors fp-swift's
/// `Result.getOrElseAsync`.
///
/// `concurrency` controls how many fallbacks run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered).
@usableFromInline
struct AsyncGetOrElseStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = Never

    @usableFromInline let onFailure: @Sendable (F) async -> Value
    @usableFromInline let concurrency: Int

    @inlinable
    init(_ onFailure: @escaping @Sendable (F) async -> Value, concurrency: Int) {
        self.onFailure = onFailure
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, Never> {
        let step = AsyncGetOrElseStep(onFailure)
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
struct AsyncGetOrElseStep<Value, F: Error>: AsyncPipeStep {
    @usableFromInline let onFailure: @Sendable (F) async -> Value

    @inlinable
    init(_ onFailure: @escaping @Sendable (F) async -> Value) {
        self.onFailure = onFailure
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) async -> Result<Value, Never>? {
        .success(await element.getOrElseAsync(onFailure))
    }
}

/// DSL: `AsyncGetOrElse { (e: AppError) async in await fetchFallback() }`.
@inlinable
public func AsyncGetOrElse<Value: Sendable, F: Error & Sendable>(
    concurrency: Int = 1,
    _ onFailure: @escaping @Sendable (F) async -> Value,
) -> some PipeFlatErrorStage<Value, F, Never> {
    AsyncGetOrElseStage(onFailure, concurrency: concurrency)
}

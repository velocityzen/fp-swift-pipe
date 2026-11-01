import FP

/// Asynchronously folds each `Result` element into a single `Output` type.
/// Mirrors fp-swift's `Result.matchAsync`.
///
/// `concurrency` controls how many folds run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered).
@usableFromInline
struct AsyncMatchStage<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>:
    PipeFoldStage
{
    @usableFromInline let onSuccess: @Sendable (Input) async -> Output
    @usableFromInline let onFailure: @Sendable (InputFailure) async -> Output
    @usableFromInline let concurrency: Int

    @inlinable
    init(
        onSuccess: @escaping @Sendable (Input) async -> Output,
        onFailure: @escaping @Sendable (InputFailure) async -> Output,
        concurrency: Int,
    ) {
        self.onSuccess = onSuccess
        self.onFailure = onFailure
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    @inlinable
    func attach(_ upstream: Pipe<Input, InputFailure>) -> Pipe<Output, Never> {
        let step = AsyncMatchStep(onSuccess: onSuccess, onFailure: onFailure)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        return .iterating {
            .init(
                upstream.compactMapAsyncUnordered(concurrency: concurrency) { element in
                    await step(element)
                }
                .makeAsyncIterator(),
            )
        }
    }
}

@usableFromInline
struct AsyncMatchStep<Input, Output, InputFailure: Error>: AsyncPipeStep {
    @usableFromInline let onSuccess: @Sendable (Input) async -> Output
    @usableFromInline let onFailure: @Sendable (InputFailure) async -> Output

    @inlinable
    init(
        onSuccess: @escaping @Sendable (Input) async -> Output,
        onFailure: @escaping @Sendable (InputFailure) async -> Output,
    ) {
        self.onSuccess = onSuccess
        self.onFailure = onFailure
    }

    @inlinable
    func callAsFunction(_ element: Result<Input, InputFailure>) async -> Result<Output, Never>? {
        .success(await element.matchAsync(onSuccess, onFailure))
    }
}

/// DSL: `AsyncMatch(onSuccess: { item in await format(item) }, onFailure: { e in await report(e) })`.
@inlinable
public func AsyncMatch<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>(
    concurrency: Int = 1,
    onSuccess: @escaping @Sendable (Input) async -> Output,
    onFailure: @escaping @Sendable (InputFailure) async -> Output,
) -> some PipeFoldStage<Input, Output, InputFailure> {
    AsyncMatchStage(onSuccess: onSuccess, onFailure: onFailure, concurrency: concurrency)
}

import FP

/// The stage behind `AsyncMatch(onSuccess:onFailure:)`.
@usableFromInline
struct AsyncMatchStage<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>:
    PipeFoldStage
{
    @usableFromInline let onSuccess: @Sendable (Input) async -> Output
    @usableFromInline let onFailure: @Sendable (InputFailure) async -> Output
    @usableFromInline let concurrency: Int
    @usableFromInline let ordered: Bool
    @usableFromInline let bufferLimit: Int

    @inlinable
    init(
        onSuccess: @escaping @Sendable (Input) async -> Output,
        onFailure: @escaping @Sendable (InputFailure) async -> Output,
        concurrency: Int,
        ordered: Bool,
        bufferLimit: Int,
    ) {
        self.onSuccess = onSuccess
        self.onFailure = onFailure
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
        self.ordered = ordered
        precondition(bufferLimit >= 0, "bufferLimit must be at least 0")
        self.bufferLimit = bufferLimit
    }

    @inlinable
    func attach(_ upstream: Pipe<Input, InputFailure>) -> Pipe<Output, Never> {
        let step = AsyncMatchStep(onSuccess: onSuccess, onFailure: onFailure)
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
                bufferLimit: bufferLimit
            ) { element in
                await step(element)
            }
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

/// Turns every element into one type with async `onSuccess` and `onFailure` closures.
///
/// The pipe cannot fail after it. Up to `concurrency` closures run at the same time, and results
/// come out in the order they finish, or in source order with `ordered: true`.
/// `bufferLimit` works as in `AsyncMap`. See <doc:HandlingFailures>.
@inlinable
public func AsyncMatch<Input: Sendable, Output: Sendable, InputFailure: Error & Sendable>(
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    onSuccess: @escaping @Sendable (Input) async -> Output,
    onFailure: @escaping @Sendable (InputFailure) async -> Output,
) -> some PipeFoldStage<Input, Output, InputFailure> {
    AsyncMatchStage(
        onSuccess: onSuccess,
        onFailure: onFailure,
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit
    )
}

import FP

/// The stage that `AsyncCompactMap` returns.
@usableFromInline
struct AsyncCompactMapStage<Input: Sendable, Output: Sendable>: PipePolyStage {
    @usableFromInline let transform: @Sendable (Input) async -> Output?
    @usableFromInline let concurrency: Int
    @usableFromInline let ordered: Bool
    @usableFromInline let bufferLimit: Int

    @inlinable
    init(
        _ transform: @escaping @Sendable (Input) async -> Output?,
        concurrency: Int,
        ordered: Bool,
        bufferLimit: Int
    ) {
        self.transform = transform
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
        self.ordered = ordered
        precondition(bufferLimit >= 0, "bufferLimit must be at least 0")
        self.bufferLimit = bufferLimit
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let step = AsyncCompactMapStep<Input, Output, F>(transform)
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
                passthrough: { $0.passthroughFailure() },
                { element in await step(element) },
            )
        }
    }
}

@usableFromInline
struct AsyncCompactMapStep<Input, Output, F: Error>: AsyncPipeStep {
    @usableFromInline let transform: @Sendable (Input) async -> Output?

    @inlinable
    init(_ transform: @escaping @Sendable (Input) async -> Output?) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(_ element: Result<Input, F>) async -> Result<Output, F>? {
        switch element {
            case .failure(let error): return .failure(error)
            case .success(let value):
                guard let mapped = await transform(value) else { return nil }
                return .success(mapped)
        }
    }
}

/// Changes each value with an async closure, and removes the `nil` results. Failures pass
/// through unchanged.
///
/// `concurrency`, `ordered` and `bufferLimit` work as in `AsyncMap`.
@inlinable
public func AsyncCompactMap<Input: Sendable, Output: Sendable>(
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    _ transform: @escaping @Sendable (Input) async -> Output?,
) -> some PipePolyStage<Input, Output> {
    AsyncCompactMapStage(
        transform,
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit
    )
}

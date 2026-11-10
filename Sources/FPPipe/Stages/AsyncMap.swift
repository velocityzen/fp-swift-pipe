import FP

/// The stage that `AsyncMap` returns.
@usableFromInline
struct AsyncMapStage<Input: Sendable, Output: Sendable>: PipePolyStage {
    @usableFromInline let transform: @Sendable (Input) async -> Output
    @usableFromInline let concurrency: Int
    @usableFromInline let ordered: Bool
    @usableFromInline let bufferLimit: Int

    @inlinable
    init(
        _ transform: @escaping @Sendable (Input) async -> Output,
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
        let step = AsyncMapStep<Input, Output, F>(transform)
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
struct AsyncMapStep<Input, Output, F: Error>: AsyncPipeStep {
    @usableFromInline let transform: @Sendable (Input) async -> Output

    @inlinable
    init(_ transform: @escaping @Sendable (Input) async -> Output) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(_ element: Result<Input, F>) async -> Result<Output, F>? {
        await element.mapAsync(transform)
    }
}

/// Changes each value with an async closure. Failures pass through unchanged.
///
/// By default, the closure handles one element at a time, in source order. With a larger
/// `concurrency`, up to that many closures run at the same time, and results come out in the
/// order they finish. `concurrency` must be 1 or more.
///
/// With `ordered: true`, results come out in source order. A slow element then holds back the
/// results after it, but not the work. `bufferLimit` caps how many results may wait for an
/// earlier, slower element. The default is no limit, and it must be 0 or more. A limit below
/// `concurrency - 1` also lowers how many closures run at the same time. See
/// <doc:AsyncAndParallelWork>.
@inlinable
public func AsyncMap<Input: Sendable, Output: Sendable>(
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    _ transform: @escaping @Sendable (Input) async -> Output,
) -> some PipePolyStage<Input, Output> {
    AsyncMapStage(transform, concurrency: concurrency, ordered: ordered, bufferLimit: bufferLimit)
}

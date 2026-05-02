import FP

/// Asynchronously maps the success channel via a non-throwing async function.
/// Failures pass through unchanged.
///
/// `concurrency` controls how many transforms run in parallel. With the default of 1
/// the closure runs strictly sequentially. With `concurrency > 1` up to N closures run
/// in flight at once and **results emit as they complete (unordered)** — for source-
/// order-preserving parallelism use `AsyncMapKeepOrder`.
struct AsyncMapStage<Input: Sendable, Output: Sendable>: PipePolyStage {
    private let transform: @Sendable (Input) async -> Output
    private let concurrency: Int

    init(_ transform: @escaping @Sendable (Input) async -> Output, concurrency: Int) {
        self.transform = transform
        self.concurrency = max(1, concurrency)
    }

    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let transform = self.transform
        let concurrency = self.concurrency
        return .erased {
            let source = upstream.upstream()
            if concurrency == 1 {
                return AnyAsyncSequence(source.mapAsync(transform))
            }
            return AnyAsyncSequence(
                mapAsyncUnordered(source, concurrency: concurrency) {
                    (element: Result<Input, F>) async -> Result<Output, F> in
                    await element.mapAsync(transform)
                },
            )
        }
    }
}

/// DSL: `AsyncMap { value in await transform(value) }` (sequential) or
/// `AsyncMap(concurrency: 10) { value in await transform(value) }` (10 in flight, unordered emit).
public func AsyncMap<Input: Sendable, Output: Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Output,
) -> some PipePolyStage<Input, Output> {
    AsyncMapStage(transform, concurrency: concurrency)
}

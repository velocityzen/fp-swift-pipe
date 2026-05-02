import FP

/// Concurrently transforms successes via an async function while **preserving source order**.
///
/// `concurrency` caps the number of in-flight transforms. With the default of 1 this is
/// strictly sequential. With `concurrency > 1` up to N closures run in parallel and results
/// emit in the original source order — slow elements hold back faster downstream ones.
struct AsyncMapKeepOrderStage<Input: Sendable, Output: Sendable>: PipePolyStage {
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
                mapAsyncKeepOrderBounded(source, concurrency: concurrency) {
                    (element: Result<Input, F>) async -> Result<Output, F> in
                    await element.mapAsync(transform)
                },
            )
        }
    }
}

/// DSL: `AsyncMapKeepOrder { url in await fetch(url) }` (sequential) or
/// `AsyncMapKeepOrder(concurrency: 10) { url in await fetch(url) }` (10 in flight, ordered emit).
public func AsyncMapKeepOrder<Input: Sendable, Output: Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Output,
) -> some PipePolyStage<Input, Output> {
    AsyncMapKeepOrderStage(transform, concurrency: concurrency)
}

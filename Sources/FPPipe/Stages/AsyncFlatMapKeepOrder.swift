import FP

/// Concurrently transforms successes via a `Result`-returning async function while
/// **preserving source order**. A `.failure` returned by the transform short-circuits
/// that element.
///
/// `concurrency` caps the number of in-flight transforms. With the default of 1 this is
/// strictly sequential. With `concurrency > 1` up to N closures run in parallel and
/// results emit in the original source order — slow elements hold back faster downstream
/// ones, but downstream sees the inputs in the order the upstream produced them.
struct AsyncFlatMapKeepOrderStage<Input: Sendable, Output: Sendable, Failure: Error & Sendable>:
    PipeStage
{
    private let transform: @Sendable (Input) async -> Result<Output, Failure>
    private let concurrency: Int

    init(
        _ transform: @escaping @Sendable (Input) async -> Result<Output, Failure>,
        concurrency: Int,
    ) {
        self.transform = transform
        self.concurrency = max(1, concurrency)
    }

    func attach(_ upstream: Pipe<Input, Failure>) -> Pipe<Output, Failure> {
        let transform = self.transform
        let concurrency = self.concurrency
        return .erased {
            let source = upstream.upstream()
            if concurrency == 1 {
                return AnyAsyncSequence(source.flatMapAsync(transform))
            }
            return AnyAsyncSequence(
                mapAsyncKeepOrderBounded(source, concurrency: concurrency) {
                    (element: Result<Input, Failure>) async -> Result<Output, Failure> in
                    await element.flatMapAsync(transform)
                },
            )
        }
    }
}

/// DSL: `AsyncFlatMapKeepOrder { url in await fetch(url) }` (sequential) or
/// `AsyncFlatMapKeepOrder(concurrency: 10) { url in await fetch(url) }`
/// (10 in flight, ordered emit, failures short-circuit per element).
public func AsyncFlatMapKeepOrder<Input: Sendable, Output: Sendable, Failure: Error & Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Result<Output, Failure>,
) -> some PipeStage<Input, Output, Failure> {
    AsyncFlatMapKeepOrderStage(transform, concurrency: concurrency)
}

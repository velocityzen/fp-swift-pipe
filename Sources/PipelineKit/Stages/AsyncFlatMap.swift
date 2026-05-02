import FP

/// Asynchronously maps the success channel via a `Result`-returning async function.
/// A `.failure` returned by the transform short-circuits that element.
///
/// `concurrency` controls how many transforms run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` up to N closures run in flight and results emit
/// as they complete (unordered).
struct AsyncFlatMapStage<Input: Sendable, Output: Sendable, Failure: Error & Sendable>:
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
                mapAsyncUnordered(source, concurrency: concurrency) {
                    (element: Result<Input, Failure>) async -> Result<Output, Failure> in
                    await element.flatMapAsync(transform)
                },
            )
        }
    }
}

/// DSL: `AsyncFlatMap { value in await fetch(value) }` or
/// `AsyncFlatMap(concurrency: 10) { value in await fetch(value) }`.
public func AsyncFlatMap<Input: Sendable, Output: Sendable, Failure: Error & Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Result<Output, Failure>,
) -> some PipeStage<Input, Output, Failure> {
    AsyncFlatMapStage(transform, concurrency: concurrency)
}

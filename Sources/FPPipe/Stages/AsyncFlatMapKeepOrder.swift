/// Use `AsyncFlatMap` with `ordered: true` instead.
@available(*, deprecated, message: "Use AsyncFlatMap with ordered: true")
@inlinable
public func AsyncFlatMapKeepOrder<Input: Sendable, Output: Sendable, Failure: Error & Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Result<Output, Failure>,
) -> some PipeStage<Input, Output, Failure> {
    AsyncFlatMap(concurrency: concurrency, ordered: true, transform)
}

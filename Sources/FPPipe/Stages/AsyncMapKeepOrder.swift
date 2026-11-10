/// Use `AsyncMap` with `ordered: true` instead.
@available(*, deprecated, message: "Use AsyncMap with ordered: true")
@inlinable
public func AsyncMapKeepOrder<Input: Sendable, Output: Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Output,
) -> some PipePolyStage<Input, Output> {
    AsyncMap(concurrency: concurrency, ordered: true, transform)
}

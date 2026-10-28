/// Asynchronously maps the success channel; produced `nil`s are dropped. Failures
/// pass through unchanged.
///
/// `concurrency` controls how many transforms run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` up to N closures run in flight and results emit
/// as they complete (unordered).
struct AsyncCompactMapStage<Input: Sendable, Output: Sendable>: PipePolyStage {
    private let transform: @Sendable (Input) async -> Output?
    private let concurrency: Int

    init(_ transform: @escaping @Sendable (Input) async -> Output?, concurrency: Int) {
        self.transform = transform
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let transform = self.transform
        let concurrency = self.concurrency
        // Shared by both branches; failures never reach it on the concurrent path,
        // which passes them through without a child task.
        let step: @Sendable (Result<Input, F>) async -> Result<Output, F>? = { element in
            switch element {
                case .failure(let error): return .failure(error)
                case .success(let value):
                    guard let mapped = await transform(value) else { return nil }
                    return .success(mapped)
            }
        }
        return .erased {
            let source = upstream.upstream()
            if concurrency == 1 {
                return AnyAsyncSequence(source.compactMap(step))
            }
            return AnyAsyncSequence(
                compactMapAsyncUnordered(
                    source,
                    concurrency: concurrency,
                    passthrough: { $0.passthroughFailure() },
                    step,
                ),
            )
        }
    }
}

/// DSL: `AsyncCompactMap { id in await fetchOrNil(id) }` or
/// `AsyncCompactMap(concurrency: 10) { id in await fetchOrNil(id) }`.
public func AsyncCompactMap<Input: Sendable, Output: Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Output?,
) -> some PipePolyStage<Input, Output> {
    AsyncCompactMapStage(transform, concurrency: concurrency)
}

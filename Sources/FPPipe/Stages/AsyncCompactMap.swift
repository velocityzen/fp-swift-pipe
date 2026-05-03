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
        self.concurrency = max(1, concurrency)
    }

    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let transform = self.transform
        let concurrency = self.concurrency
        return .erased {
            let source = upstream.upstream()
            if concurrency == 1 {
                return AnyAsyncSequence(
                    source.compactMap { (element: Result<Input, F>) async -> Result<Output, F>? in
                        switch element {
                            case .failure(let error): return .failure(error)
                            case .success(let value):
                                guard let mapped = await transform(value) else { return nil }
                                return .success(mapped)
                        }
                    },
                )
            }
            return AnyAsyncSequence(
                compactMapAsyncUnordered(source, concurrency: concurrency) {
                    (element: Result<Input, F>) async -> Result<Output, F>? in
                    switch element {
                        case .failure(let error): return .failure(error)
                        case .success(let value):
                            guard let mapped = await transform(value) else { return nil }
                            return .success(mapped)
                    }
                },
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

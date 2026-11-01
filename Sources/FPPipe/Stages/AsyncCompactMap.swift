import FP

/// Asynchronously maps the success channel; produced `nil`s are dropped. Failures
/// pass through unchanged.
///
/// `concurrency` controls how many transforms run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` up to N closures run in flight and results emit
/// as they complete (unordered).
@usableFromInline
struct AsyncCompactMapStage<Input: Sendable, Output: Sendable>: PipePolyStage {
    @usableFromInline let transform: @Sendable (Input) async -> Output?
    @usableFromInline let concurrency: Int

    @inlinable
    init(_ transform: @escaping @Sendable (Input) async -> Output?, concurrency: Int) {
        self.transform = transform
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let step = AsyncCompactMapStep<Input, Output, F>(transform)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        return .iterating {
            .init(
                upstream.compactMapAsyncUnordered(
                    concurrency: concurrency,
                    passthrough: { $0.passthroughFailure() },
                    { element in await step(element) },
                )
                .makeAsyncIterator(),
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

/// DSL: `AsyncCompactMap { id in await fetchOrNil(id) }` or
/// `AsyncCompactMap(concurrency: 10) { id in await fetchOrNil(id) }`.
@inlinable
public func AsyncCompactMap<Input: Sendable, Output: Sendable>(
    concurrency: Int = 1,
    _ transform: @escaping @Sendable (Input) async -> Output?,
) -> some PipePolyStage<Input, Output> {
    AsyncCompactMapStage(transform, concurrency: concurrency)
}

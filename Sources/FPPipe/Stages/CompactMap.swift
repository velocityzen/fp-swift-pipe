/// Maps the success channel; produced `nil`s are dropped. Failures pass through.
@usableFromInline
struct CompactMapStage<Input: Sendable, Output: Sendable>: PipePolyStage {
    @usableFromInline let transform: @Sendable (Input) -> Output?

    @inlinable
    init(_ transform: @escaping @Sendable (Input) -> Output?) {
        self.transform = transform
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let step = CompactMapStep<Input, Output, F>(transform)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct CompactMapStep<Input, Output, F: Error>: PipeStep {
    @usableFromInline let transform: @Sendable (Input) -> Output?

    @inlinable
    init(_ transform: @escaping @Sendable (Input) -> Output?) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(_ element: Result<Input, F>) -> Result<Output, F>? {
        switch element {
            case .failure(let error): return .failure(error)
            case .success(let value):
                guard let mapped = transform(value) else { return nil }
                return .success(mapped)
        }
    }
}

/// DSL: `CompactMap { Int($0) }`.
@inlinable
public func CompactMap<Input: Sendable, Output: Sendable>(
    _ transform: @escaping @Sendable (Input) -> Output?,
) -> some PipePolyStage<Input, Output> {
    CompactMapStage(transform)
}

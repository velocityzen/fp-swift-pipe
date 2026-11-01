/// Maps the success channel via a pure function. Failures pass through unchanged.
///
/// `Map` is failure-polymorphic — it works with any failure type the upstream defines.
@usableFromInline
struct MapStage<Input: Sendable, Output: Sendable>: PipePolyStage {
    @usableFromInline let transform: @Sendable (Input) -> Output

    @inlinable
    init(_ transform: @escaping @Sendable (Input) -> Output) {
        self.transform = transform
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let step = MapStep<Input, Output, F>(transform)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct MapStep<Input, Output, F: Error>: PipeStep {
    @usableFromInline let transform: @Sendable (Input) -> Output

    @inlinable
    init(_ transform: @escaping @Sendable (Input) -> Output) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(_ element: Result<Input, F>) -> Result<Output, F>? {
        element.map(transform)
    }
}

/// DSL: `Map { $0 + 1 }`.
@inlinable
public func Map<Input: Sendable, Output: Sendable>(
    _ transform: @escaping @Sendable (Input) -> Output,
) -> some PipePolyStage<Input, Output> {
    MapStage(transform)
}

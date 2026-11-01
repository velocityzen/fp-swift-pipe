/// Maps the success channel via a `Result`-returning function. Failures pass through;
/// a `.failure` returned by `transform` short-circuits the success channel for that element.
@usableFromInline
struct FlatMapStage<Input: Sendable, Output: Sendable, Failure: Error & Sendable>: PipeStage {
    @usableFromInline let transform: @Sendable (Input) -> Result<Output, Failure>

    @inlinable
    init(_ transform: @escaping @Sendable (Input) -> Result<Output, Failure>) {
        self.transform = transform
    }

    @inlinable
    func attach(_ upstream: Pipe<Input, Failure>) -> Pipe<Output, Failure> {
        let step = FlatMapStep(transform)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct FlatMapStep<Input, Output, Failure: Error>: PipeStep {
    @usableFromInline let transform: @Sendable (Input) -> Result<Output, Failure>

    @inlinable
    init(_ transform: @escaping @Sendable (Input) -> Result<Output, Failure>) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(_ element: Result<Input, Failure>) -> Result<Output, Failure>? {
        element.flatMap(transform)
    }
}

/// DSL: `FlatMap { value in .success(value * 2) }`.
@inlinable
public func FlatMap<Input: Sendable, Output: Sendable, Failure: Error & Sendable>(
    _ transform: @escaping @Sendable (Input) -> Result<Output, Failure>,
) -> some PipeStage<Input, Output, Failure> {
    FlatMapStage(transform)
}

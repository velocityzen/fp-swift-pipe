import FP

/// Maps the success channel via a `Result`-returning function. Failures pass through;
/// a `.failure` returned by `transform` short-circuits the success channel for that element.
struct FlatMapStage<Input: Sendable, Output: Sendable, Failure: Error & Sendable>: PipeStage {
    private let transform: @Sendable (Input) -> Result<Output, Failure>

    init(_ transform: @escaping @Sendable (Input) -> Result<Output, Failure>) {
        self.transform = transform
    }

    func attach(_ upstream: Pipe<Input, Failure>) -> Pipe<Output, Failure> {
        let transform = self.transform
        return .erased { AnyAsyncSequence(upstream.upstream().flatMap(transform)) }
    }
}

/// DSL: `FlatMap { value in .success(value * 2) }`.
public func FlatMap<Input: Sendable, Output: Sendable, Failure: Error & Sendable>(
    _ transform: @escaping @Sendable (Input) -> Result<Output, Failure>,
) -> some PipeStage<Input, Output, Failure> {
    FlatMapStage(transform)
}

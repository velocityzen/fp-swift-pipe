/// Maps the success channel; produced `nil`s are dropped. Failures pass through.
struct CompactMapStage<Input: Sendable, Output: Sendable>: PipePolyStage {
    private let transform: @Sendable (Input) -> Output?

    init(_ transform: @escaping @Sendable (Input) -> Output?) {
        self.transform = transform
    }

    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let transform = self.transform
        return .erased {
            AnyAsyncSequence(
                upstream.upstream().compactMap {
                    (element: Result<Input, F>) -> Result<Output, F>? in
                    switch element {
                        case .failure(let error): return .failure(error)
                        case .success(let value):
                            guard let mapped = transform(value) else { return nil }
                            return .success(mapped)
                    }
                },
            )
        }
    }
}

/// DSL: `CompactMap { Int($0) }`.
public func CompactMap<Input: Sendable, Output: Sendable>(
    _ transform: @escaping @Sendable (Input) -> Output?,
) -> some PipePolyStage<Input, Output> {
    CompactMapStage(transform)
}

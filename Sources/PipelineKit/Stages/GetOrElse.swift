import FP

/// Replace each `.failure` with a `Success` derived from the error. The output
/// pipeline cannot fail (`Failure == Never`). Mirrors fp-swift's `Result.getOrElse`.
struct GetOrElseStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    typealias InputFailure = F
    typealias OutputFailure = Never

    private let onFailure: @Sendable (F) -> Value

    init(_ onFailure: @escaping @Sendable (F) -> Value) {
        self.onFailure = onFailure
    }

    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, Never> {
        let onFailure = self.onFailure
        return .erased {
            AnyAsyncSequence(
                upstream.upstream().map { (element: Result<Value, F>) -> Result<Value, Never> in
                    .success(element.getOrElse(onFailure))
                },
            )
        }
    }
}

/// DSL: `GetOrElse { (e: AppError) in fallbackItem }`.
public func GetOrElse<Value: Sendable, F: Error & Sendable>(
    _ onFailure: @escaping @Sendable (F) -> Value,
) -> some PipeFlatErrorStage<Value, F, Never> {
    GetOrElseStage(onFailure)
}

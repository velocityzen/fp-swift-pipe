import FP

/// The stage that `AsyncGetOrElse` returns.
@usableFromInline
struct AsyncGetOrElseStage<Value: Sendable, F: Error & Sendable>: PipeFlatErrorStage {
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = Never

    @usableFromInline let onFailure: @Sendable (F) async -> Value
    @usableFromInline let concurrency: Int
    @usableFromInline let ordered: Bool
    @usableFromInline let bufferLimit: Int

    @inlinable
    init(
        _ onFailure: @escaping @Sendable (F) async -> Value,
        concurrency: Int,
        ordered: Bool,
        bufferLimit: Int
    ) {
        self.onFailure = onFailure
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
        self.ordered = ordered
        precondition(bufferLimit >= 0, "bufferLimit must be at least 0")
        self.bufferLimit = bufferLimit
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, Never> {
        let step = AsyncGetOrElseStep(onFailure)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        let concurrency = self.concurrency
        let ordered = self.ordered
        let bufferLimit = self.bufferLimit
        return .iterating {
            upstream.compactMapAsyncIterator(
                concurrency: concurrency,
                ordered: ordered,
                bufferLimit: bufferLimit,
                passthrough: { $0.passthroughSuccess() },
                { element in await step(element) },
            )
        }
    }
}

@usableFromInline
struct AsyncGetOrElseStep<Value, F: Error>: AsyncPipeStep {
    @usableFromInline let onFailure: @Sendable (F) async -> Value

    @inlinable
    init(_ onFailure: @escaping @Sendable (F) async -> Value) {
        self.onFailure = onFailure
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, F>) async -> Result<Value, Never>? {
        .success(await element.getOrElseAsync(onFailure))
    }
}

/// Replaces each failure with a value that an async closure makes from the error.
///
/// After this stage, the pipe cannot fail. `concurrency`, `ordered` and `bufferLimit` work as in
/// `AsyncMap`.
@inlinable
public func AsyncGetOrElse<Value: Sendable, F: Error & Sendable>(
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    _ onFailure: @escaping @Sendable (F) async -> Value,
) -> some PipeFlatErrorStage<Value, F, Never> {
    AsyncGetOrElseStage(
        onFailure,
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit
    )
}

/// The stage that `MapError` returns.
@usableFromInline
struct MapErrorStage<
    InputFailure: Error & Sendable,
    OutputFailure: Error & Sendable,
>: PipePolyValueStage {
    @usableFromInline let transform: @Sendable (InputFailure) -> OutputFailure

    @inlinable
    init(_ transform: @escaping @Sendable (InputFailure) -> OutputFailure) {
        self.transform = transform
    }

    @inlinable
    func attach<V: Sendable>(_ upstream: Pipe<V, InputFailure>) -> Pipe<V, OutputFailure> {
        let step = MapErrorStep<V, InputFailure, OutputFailure>(transform)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

@usableFromInline
struct MapErrorStep<Value, InputFailure: Error, OutputFailure: Error>: PipeStep {
    @usableFromInline let transform: @Sendable (InputFailure) -> OutputFailure

    @inlinable
    init(_ transform: @escaping @Sendable (InputFailure) -> OutputFailure) {
        self.transform = transform
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, InputFailure>) -> Result<Value, OutputFailure>? {
        element.mapError(transform)
    }
}

/// Changes each error, or its type. Successes pass through unchanged.
@inlinable
public func MapError<InputFailure: Error & Sendable, OutputFailure: Error & Sendable>(
    _ transform: @escaping @Sendable (InputFailure) -> OutputFailure,
) -> some PipePolyValueStage<InputFailure, OutputFailure> {
    MapErrorStage(transform)
}

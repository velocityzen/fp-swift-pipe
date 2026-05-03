/// Result builder for declaring pipelines as a sequence of a source followed by stages.
///
/// The builder uses `buildPartialBlock` to left-fold heterogeneous steps:
/// the first step must be a `PipeSource`, and each subsequent step is a
/// `PipeStage` whose `Input` matches the upstream's `Output`.
@resultBuilder
public enum PipeBuilder {
    // MARK: - First step (the source)

    public static func buildPartialBlock<S: PipeSource>(
        first source: S,
    ) -> Pipe<S.Output, S.Failure> {
        source.produce()
    }

    // MARK: - Subsequent steps (stages)

    public static func buildPartialBlock<U: Sendable, St: PipeStage>(
        accumulated: Pipe<U, St.Failure>,
        next stage: St,
    ) -> Pipe<St.Output, St.Failure> where St.Input == U {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<U: Sendable, F: Error & Sendable, St: PipePolyStage>(
        accumulated: Pipe<U, F>,
        next stage: St,
    ) -> Pipe<St.Output, F> where St.Input == U {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<V: Sendable, St: PipePolyValueStage>(
        accumulated: Pipe<V, St.InputFailure>,
        next stage: St,
    ) -> Pipe<V, St.OutputFailure> {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<
        V: Sendable,
        F: Error & Sendable,
    >(
        accumulated: Pipe<V, F>,
        next stage: some PipeForwardingStage,
    ) -> Pipe<V, F> {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<St: PipeFlatErrorStage>(
        accumulated: Pipe<St.Value, St.InputFailure>,
        next stage: St,
    ) -> Pipe<St.Value, St.OutputFailure> {
        stage.attach(accumulated)
    }

    public static func buildPartialBlock<St: PipeFoldStage>(
        accumulated: Pipe<St.Input, St.InputFailure>,
        next stage: St,
    ) -> Pipe<St.Output, Never> {
        stage.attach(accumulated)
    }

    /// Widening overload: when the upstream cannot fail (`Failure == Never`), allow
    /// attaching a failure-fixed stage by lifting the failure channel into `St.Failure`.
    /// This makes `From([…]) → FlatMap { … }` work without an explicit `MapError` step.
    public static func buildPartialBlock<U: Sendable, St: PipeStage>(
        accumulated: Pipe<U, Never>,
        next stage: St,
    ) -> Pipe<St.Output, St.Failure> where St.Input == U {
        stage.attach(accumulated.widenFailure(to: St.Failure.self))
    }

    /// Widening overload for value-polymorphic failure-transforming stages (e.g. `MapError`).
    public static func buildPartialBlock<V: Sendable, St: PipePolyValueStage>(
        accumulated: Pipe<V, Never>,
        next stage: St,
    ) -> Pipe<V, St.OutputFailure> where St.InputFailure == Never {
        stage.attach(accumulated)
    }

    // MARK: - Open-pipe variants
    //
    // These mirror the closed-pipe overloads but accumulate an `OpenPipe` whose
    // source slot is filled later by `OpenPipe.callAsFunction(_:)`. Each overload
    // composes the new stage into the accumulated function.

    public static func buildPartialBlock<I: Sendable>(
        first: OpenSource<I>,
    ) -> OpenPipe<I, I, Never> {
        OpenPipe(apply: { $0 })
    }

    public static func buildPartialBlock<I: Sendable, U: Sendable, St: PipeStage>(
        accumulated: OpenPipe<I, U, St.Failure>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, St.Failure> where St.Input == U {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    public static func buildPartialBlock<
        I: Sendable,
        U: Sendable,
        F: Error & Sendable,
        St: PipePolyStage,
    >(
        accumulated: OpenPipe<I, U, F>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, F> where St.Input == U {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    public static func buildPartialBlock<I: Sendable, V: Sendable, St: PipePolyValueStage>(
        accumulated: OpenPipe<I, V, St.InputFailure>,
        next stage: St,
    ) -> OpenPipe<I, V, St.OutputFailure> {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    public static func buildPartialBlock<I: Sendable, V: Sendable, F: Error & Sendable>(
        accumulated: OpenPipe<I, V, F>,
        next stage: some PipeForwardingStage,
    ) -> OpenPipe<I, V, F> {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    public static func buildPartialBlock<I: Sendable, St: PipeFlatErrorStage>(
        accumulated: OpenPipe<I, St.Value, St.InputFailure>,
        next stage: St,
    ) -> OpenPipe<I, St.Value, St.OutputFailure> {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    public static func buildPartialBlock<I: Sendable, St: PipeFoldStage>(
        accumulated: OpenPipe<I, St.Input, St.InputFailure>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, Never> {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    /// Widening overload (open-pipe form): non-failable upstream → failure-fixed stage.
    public static func buildPartialBlock<I: Sendable, U: Sendable, St: PipeStage>(
        accumulated: OpenPipe<I, U, Never>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, St.Failure> where St.Input == U {
        OpenPipe(apply: { stage.attach(accumulated.apply($0).widenFailure(to: St.Failure.self)) })
    }

    /// Widening overload (open-pipe form): non-failable upstream → value-poly failure stage.
    public static func buildPartialBlock<I: Sendable, V: Sendable, St: PipePolyValueStage>(
        accumulated: OpenPipe<I, V, Never>,
        next stage: St,
    ) -> OpenPipe<I, V, St.OutputFailure> where St.InputFailure == Never {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }
}

// MARK: - Pipe initializer

public extension Pipe {
    /// Build a pipeline from a sequence of a source and stages.
    init(@PipeBuilder _ build: () -> Pipe<Success, Failure>) {
        self = build()
    }
}

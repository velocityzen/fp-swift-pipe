/// Result builder for declaring pipelines as a sequence of a source followed by stages.
///
/// The builder uses `buildPartialBlock` to left-fold heterogeneous steps:
/// the first step must be a `PipeSource`, and each subsequent step is a
/// `PipeStage` whose `Input` matches the upstream's `Output`.
///
/// ## Extending the builder
///
/// Adding a new stage shape (rare) requires touching this file in a predictable matrix:
/// for each new stage protocol you typically add overloads across these axes:
///
/// 1. **First-step identity** (`Pipe`-side, ~6 lines): `buildPartialBlock(first stage:)` —
///    lets a single stage appear as the body of an `if/else` / `switch case` branch.
/// 2. **Accumulated × stage** (`Pipe`-side, ~6 lines): combines an existing `Pipe<U, F>`
///    with the new stage's `attach`.
/// 3. **Open-pipe variant** (~6 lines, mirrors closed): same combine, but the accumulator
///    is `OpenPipe<I, U, F>` and stages compose into the open-pipe's `apply` closure.
/// 4. **Widening** (only if the new stage requires a bound failure channel): a `Never`-input
///    overload that lifts via `widenFailure(to:)`. If the stage's failure can itself be
///    `Never`, also add a `Never`-to-`Never` overload constrained on both sides — the plain
///    and widening overloads both accept that pair, and neither is more specific.
/// 5. **Optional** (only if the stage is type-preserving): a `next: OptionalStage<St>`
///    overload that returns `accumulated` unchanged when `optional.stage == nil`.
///
/// Roughly 4-6 overloads per new stage protocol. The MARK sections below group by axis;
/// new overloads belong in the matching section.
@resultBuilder
public enum PipeBuilder {
    // MARK: - First step (the source)

    @inlinable
    public static func buildPartialBlock<S: PipeSource>(
        first source: S,
    ) -> Pipe<S.Output, S.Failure> {
        source.produce()
    }

    // MARK: - Subsequent steps (stages)

    @inlinable
    public static func buildPartialBlock<U: Sendable, St: PipeStage>(
        accumulated: Pipe<U, St.Failure>,
        next stage: St,
    ) -> Pipe<St.Output, St.Failure> where St.Input == U {
        stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<U: Sendable, F: Error & Sendable, St: PipePolyStage>(
        accumulated: Pipe<U, F>,
        next stage: St,
    ) -> Pipe<St.Output, F> where St.Input == U {
        stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<V: Sendable, St: PipePolyValueStage>(
        accumulated: Pipe<V, St.InputFailure>,
        next stage: St,
    ) -> Pipe<V, St.OutputFailure> {
        stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<
        V: Sendable,
        F: Error & Sendable,
    >(
        accumulated: Pipe<V, F>,
        next stage: some PipeForwardingStage,
    ) -> Pipe<V, F> {
        stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<St: PipeFlatErrorStage>(
        accumulated: Pipe<St.Value, St.InputFailure>,
        next stage: St,
    ) -> Pipe<St.Value, St.OutputFailure> {
        stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<St: PipeFoldStage>(
        accumulated: Pipe<St.Input, St.InputFailure>,
        next stage: St,
    ) -> Pipe<St.Output, Never> {
        stage.attach(accumulated)
    }

    /// Widening overload: when the upstream cannot fail (`Failure == Never`), allow
    /// attaching a failure-fixed stage by lifting the failure channel into `St.Failure`.
    /// This makes `From([…]) → FlatMap { … }` work without an explicit `MapError` step.
    @inlinable
    public static func buildPartialBlock<U: Sendable, St: PipeStage>(
        accumulated: Pipe<U, Never>,
        next stage: St,
    ) -> Pipe<St.Output, St.Failure> where St.Input == U {
        stage.attach(accumulated.widenFailure(to: St.Failure.self))
    }

    /// Widening overload for value-polymorphic failure-transforming stages (e.g. `MapError`).
    @inlinable
    public static func buildPartialBlock<V: Sendable, St: PipePolyValueStage>(
        accumulated: Pipe<V, Never>,
        next stage: St,
    ) -> Pipe<V, St.OutputFailure> where St.InputFailure == Never {
        stage.attach(accumulated)
    }

    /// A failure-fixed stage that cannot fail (`St.Failure == Never`, e.g. `FlatMap` to
    /// `Result<_, Never>`) after an upstream that cannot fail either: nothing to widen.
    /// Both the plain overload and the widening one accept this pair and neither is more
    /// specific than the other, so without this one the call is ambiguous.
    @inlinable
    public static func buildPartialBlock<U: Sendable, St: PipeStage>(
        accumulated: Pipe<U, Never>,
        next stage: St,
    ) -> Pipe<St.Output, Never> where St.Input == U, St.Failure == Never {
        stage.attach(accumulated)
    }
}

extension PipeBuilder {
    // MARK: - Open-pipe variants
    //
    // These mirror the closed-pipe overloads but accumulate an `OpenPipe` whose
    // source slot is filled later by `OpenPipe.callAsFunction(_:)`. Each overload
    // composes the new stage into the accumulated function.

    @inlinable
    public static func buildPartialBlock<I: Sendable>(
        first: OpenSource<I>,
    ) -> OpenPipe<I, I, Never> {
        OpenPipe(apply: { $0 })
    }

    /// Result-bearing open source: callers will supply an `AsyncSequence<Result<V, E>>`,
    /// and the inner `Result`s lift into the channel so downstream sees `Pipe<V, E>`.
    @inlinable
    public static func buildPartialBlock<V: Sendable, E: Error & Sendable>(
        first: OpenResultSource<V, E>,
    ) -> OpenPipe<Result<V, E>, V, E> {
        OpenPipe(apply: { lifted in
            Pipe<V, E>.iterating {
                .init(StepIterator(lifted.makeAsyncIterator(), step: UnwrapResultStep<V, E>()))
            }
        })
    }

    @inlinable
    public static func buildPartialBlock<I: Sendable, U: Sendable, St: PipeStage>(
        accumulated: OpenPipe<I, U, St.Failure>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, St.Failure> where St.Input == U {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    @inlinable
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

    @inlinable
    public static func buildPartialBlock<I: Sendable, V: Sendable, St: PipePolyValueStage>(
        accumulated: OpenPipe<I, V, St.InputFailure>,
        next stage: St,
    ) -> OpenPipe<I, V, St.OutputFailure> {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    @inlinable
    public static func buildPartialBlock<I: Sendable, V: Sendable, F: Error & Sendable>(
        accumulated: OpenPipe<I, V, F>,
        next stage: some PipeForwardingStage,
    ) -> OpenPipe<I, V, F> {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    @inlinable
    public static func buildPartialBlock<I: Sendable, St: PipeFlatErrorStage>(
        accumulated: OpenPipe<I, St.Value, St.InputFailure>,
        next stage: St,
    ) -> OpenPipe<I, St.Value, St.OutputFailure> {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    @inlinable
    public static func buildPartialBlock<I: Sendable, St: PipeFoldStage>(
        accumulated: OpenPipe<I, St.Input, St.InputFailure>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, Never> {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    /// Widening overload (open-pipe form): non-failable upstream → failure-fixed stage.
    @inlinable
    public static func buildPartialBlock<I: Sendable, U: Sendable, St: PipeStage>(
        accumulated: OpenPipe<I, U, Never>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, St.Failure> where St.Input == U {
        OpenPipe(apply: { stage.attach(accumulated.apply($0).widenFailure(to: St.Failure.self)) })
    }

    /// Widening overload (open-pipe form): non-failable upstream → value-poly failure stage.
    @inlinable
    public static func buildPartialBlock<I: Sendable, V: Sendable, St: PipePolyValueStage>(
        accumulated: OpenPipe<I, V, Never>,
        next stage: St,
    ) -> OpenPipe<I, V, St.OutputFailure> where St.InputFailure == Never {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    /// Open-pipe form of the `Never`-to-`Never` overload: a stage that cannot fail after an
    /// upstream that cannot fail, which the plain and widening overloads both accept.
    @inlinable
    public static func buildPartialBlock<I: Sendable, U: Sendable, St: PipeStage>(
        accumulated: OpenPipe<I, U, Never>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, Never> where St.Input == U, St.Failure == Never {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    // MARK: - Stage-only block bodies (for branch / loop bodies)
    //
    // The body of an `if`, `else`, `switch case`, or `for` is itself processed by this
    // builder. When such a body is a single stage (no source), we pass the stage through
    // unchanged. The stage is then applied by the outer accumulator's `buildPartialBlock`.

    @inlinable
    public static func buildPartialBlock<St: PipeStage>(first stage: St) -> St { stage }
    @inlinable
    public static func buildPartialBlock<St: PipePolyStage>(first stage: St) -> St { stage }
    @inlinable
    public static func buildPartialBlock<St: PipePolyValueStage>(first stage: St) -> St { stage }
    @inlinable
    public static func buildPartialBlock<St: PipeForwardingStage>(first stage: St) -> St { stage }
    @inlinable
    public static func buildPartialBlock<St: PipeFlatErrorStage>(first stage: St) -> St { stage }
    @inlinable
    public static func buildPartialBlock<St: PipeFoldStage>(first stage: St) -> St { stage }

    // MARK: - Conditional composition (`if/else`, `switch`)
    //
    // Both branches must produce a value of the same Swift type. In practice that means
    // the same stage shape with the same Input/Output/Failure. Mixing stage protocols
    // (e.g. Map in one branch, Filter in another) won't unify — Swift catches this.

    @inlinable
    public static func buildEither<C>(first component: C) -> C { component }
    @inlinable
    public static func buildEither<C>(second component: C) -> C { component }

    // MARK: - Optional composition (`if` without `else`)
    //
    // `buildOptional` wraps the body's stage in `OptionalStage<St>`. The outer
    // `buildPartialBlock` overloads applies the stage if present, passes through if absent.
    // Only type-preserving stages are allowed — otherwise the absent case couldn't yield
    // the same Pipe type as the present case.

    @inlinable
    public static func buildOptional<St>(_ component: St?) -> OptionalStage<St> {
        OptionalStage(stage: component)
    }

    @inlinable
    public static func buildPartialBlock<V: Sendable, F: Error & Sendable, St: PipeForwardingStage>(
        accumulated: Pipe<V, F>,
        next optional: OptionalStage<St>,
    ) -> Pipe<V, F> {
        guard let stage = optional.stage else { return accumulated }
        return stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<U: Sendable, F: Error & Sendable, St: PipePolyStage>(
        accumulated: Pipe<U, F>,
        next optional: OptionalStage<St>,
    ) -> Pipe<U, F> where St.Input == U, St.Output == U {
        guard let stage = optional.stage else { return accumulated }
        return stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<V: Sendable, St: PipePolyValueStage>(
        accumulated: Pipe<V, St.InputFailure>,
        next optional: OptionalStage<St>,
    ) -> Pipe<V, St.InputFailure> where St.InputFailure == St.OutputFailure {
        guard let stage = optional.stage else { return accumulated }
        return stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<U: Sendable, St: PipeStage>(
        accumulated: Pipe<U, St.Failure>,
        next optional: OptionalStage<St>,
    ) -> Pipe<U, St.Failure> where St.Input == U, St.Output == U {
        guard let stage = optional.stage else { return accumulated }
        return stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<St: PipeFlatErrorStage>(
        accumulated: Pipe<St.Value, St.InputFailure>,
        next optional: OptionalStage<St>,
    ) -> Pipe<St.Value, St.InputFailure> where St.InputFailure == St.OutputFailure {
        guard let stage = optional.stage else { return accumulated }
        return stage.attach(accumulated)
    }

    @inlinable
    public static func buildPartialBlock<
        I: Sendable,
        V: Sendable,
        F: Error & Sendable,
        St: PipeForwardingStage,
    >(
        accumulated: OpenPipe<I, V, F>,
        next optional: OptionalStage<St>,
    ) -> OpenPipe<I, V, F> {
        OpenPipe(apply: { input in
            let acc = accumulated.apply(input)
            guard let stage = optional.stage else { return acc }
            return stage.attach(acc)
        })
    }

    @inlinable
    public static func buildPartialBlock<
        I: Sendable,
        U: Sendable,
        F: Error & Sendable,
        St: PipePolyStage,
    >(
        accumulated: OpenPipe<I, U, F>,
        next optional: OptionalStage<St>,
    ) -> OpenPipe<I, U, F> where St.Input == U, St.Output == U {
        OpenPipe(apply: { input in
            let acc = accumulated.apply(input)
            guard let stage = optional.stage else { return acc }
            return stage.attach(acc)
        })
    }

    @inlinable
    public static func buildPartialBlock<I: Sendable, V: Sendable, St: PipePolyValueStage>(
        accumulated: OpenPipe<I, V, St.InputFailure>,
        next optional: OptionalStage<St>,
    ) -> OpenPipe<I, V, St.InputFailure> where St.InputFailure == St.OutputFailure {
        OpenPipe(apply: { input in
            let acc = accumulated.apply(input)
            guard let stage = optional.stage else { return acc }
            return stage.attach(acc)
        })
    }

    @inlinable
    public static func buildPartialBlock<I: Sendable, U: Sendable, St: PipeStage>(
        accumulated: OpenPipe<I, U, St.Failure>,
        next optional: OptionalStage<St>,
    ) -> OpenPipe<I, U, St.Failure> where St.Input == U, St.Output == U {
        OpenPipe(apply: { input in
            let acc = accumulated.apply(input)
            guard let stage = optional.stage else { return acc }
            return stage.attach(acc)
        })
    }

    @inlinable
    public static func buildPartialBlock<I: Sendable, St: PipeFlatErrorStage>(
        accumulated: OpenPipe<I, St.Value, St.InputFailure>,
        next optional: OptionalStage<St>,
    ) -> OpenPipe<I, St.Value, St.InputFailure> where St.InputFailure == St.OutputFailure {
        OpenPipe(apply: { input in
            let acc = accumulated.apply(input)
            guard let stage = optional.stage else { return acc }
            return stage.attach(acc)
        })
    }

    // Note on `for` loops: deliberately not supported. `buildArray` would need
    // `[some PipePolyStage<I, O>]`, but the Swift compiler can't infer the underlying
    // opaque type when the loop body produces stages whose factories return
    // `some Protocol`. Compose loops outside the builder by reducing into a single stage
    // (e.g. `Map(transforms.reduce(id, compose))`).
}

/// Wrapper produced by `PipeBuilder.buildOptional` so the outer fold can recognise an
/// "optional stage" position. Only type-preserving stages can be optional, since the
/// absent case must yield the same Pipe type as the present case.
public struct OptionalStage<St>: Sendable where St: Sendable {
    @usableFromInline let stage: St?

    @inlinable
    init(stage: St?) {
        self.stage = stage
    }
}

/// Unwraps the `Result` elements an `OpenResultSource` pipe was called with into the
/// pipe's own channels.
@usableFromInline
struct UnwrapResultStep<V, E: Error>: PipeStep {
    @inlinable
    init() {}

    @inlinable
    func callAsFunction(_ element: Result<Result<V, E>, Never>) -> Result<V, E>? {
        switch element {
            case .success(let inner): inner
        }
    }
}

// MARK: - Pipe initializer

public extension Pipe {
    /// Build a pipeline from a sequence of a source and stages.
    @inlinable
    init(@PipeBuilder _ build: () -> Pipe<Success, Failure>) {
        self = build()
    }
}

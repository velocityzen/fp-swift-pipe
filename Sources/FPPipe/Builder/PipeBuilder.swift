// Extending the builder
//
// A stage that uses an existing stage protocol needs no new overloads. A new stage protocol
// (rare) needs overloads on these axes:
//
// 1. First-step identity: `buildPartialBlock(first stage:)`. It lets one stage be the body of
//    an `if`/`else` branch or a `switch` case.
// 2. Accumulated × stage: joins a `Pipe<U, F>` with the new stage's `attach`.
// 3. Open-pipe variant: the same join, but the accumulated value is an `OpenPipe<I, U, F>`,
//    and the stage goes into the open pipe's `apply` closure.
// 4. Widening (only when the stage fixes the failure type): an overload for a `Never` input
//    that uses `widenFailure(to:)`. If the stage's own failure can be `Never`, also add a
//    `Never` to `Never` overload. Without it, the plain and widening overloads both match that
//    pair, and neither is more specific.
// 5. Optional (only when the stage keeps the types the same): a `next: OptionalStage<St>`
//    overload that returns `accumulated` unchanged when `optional.stage == nil`.
//
// That is about 4 to 6 overloads for each new stage protocol. Put each new overload in the
// matching MARK section below.

/// Builds a pipe from a source and stages.
///
/// The first line must be a source, like `From`. Each line after it is a stage. A source that
/// cannot fail works with stages that can fail. You can use `if`, `if`/`else` and `switch`, but
/// not `for`. See <doc:ConditionsInsideAPipe>.
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

    /// Lets a stage that can fail, like `FlatMap`, come after steps that cannot fail.
    @inlinable
    public static func buildPartialBlock<U: Sendable, St: PipeStage>(
        accumulated: Pipe<U, Never>,
        next stage: St,
    ) -> Pipe<St.Output, St.Failure> where St.Input == U {
        stage.attach(accumulated.widenFailure(to: St.Failure.self))
    }

    /// Lets an error stage, like `MapError`, come after steps that cannot fail.
    @inlinable
    public static func buildPartialBlock<V: Sendable, St: PipePolyValueStage>(
        accumulated: Pipe<V, Never>,
        next stage: St,
    ) -> Pipe<V, St.OutputFailure> where St.InputFailure == Never {
        stage.attach(accumulated)
    }

    /// Adds a stage that cannot fail after steps that cannot fail. Without it, the call is
    /// ambiguous.
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
    // The same overloads as above, for an `OpenPipe`. Each one adds the stage to the open
    // pipe's `apply` function. `OpenPipe.callAsFunction(_:)` gives the source later.

    @inlinable
    public static func buildPartialBlock<I: Sendable>(
        first: OpenSource<I>,
    ) -> OpenPipe<I, I, Never> {
        OpenPipe(apply: { $0 })
    }

    /// Starts an open pipe that reads `Result`s. Input failures go into the failure channel.
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

    /// The open-pipe form of the `FlatMap` widening overload.
    @inlinable
    public static func buildPartialBlock<I: Sendable, U: Sendable, St: PipeStage>(
        accumulated: OpenPipe<I, U, Never>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, St.Failure> where St.Input == U {
        OpenPipe(apply: { stage.attach(accumulated.apply($0).widenFailure(to: St.Failure.self)) })
    }

    /// The open-pipe form of the `MapError` widening overload.
    @inlinable
    public static func buildPartialBlock<I: Sendable, V: Sendable, St: PipePolyValueStage>(
        accumulated: OpenPipe<I, V, Never>,
        next stage: St,
    ) -> OpenPipe<I, V, St.OutputFailure> where St.InputFailure == Never {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    /// The open-pipe form of the `Never` to `Never` overload.
    @inlinable
    public static func buildPartialBlock<I: Sendable, U: Sendable, St: PipeStage>(
        accumulated: OpenPipe<I, U, Never>,
        next stage: St,
    ) -> OpenPipe<I, St.Output, Never> where St.Input == U, St.Failure == Never {
        OpenPipe(apply: { stage.attach(accumulated.apply($0)) })
    }

    // MARK: - Stage-only block bodies (for branch bodies)
    //
    // The body of an `if`, `else` or `switch` case goes through this builder too. When the
    // body is one stage with no source, it passes through unchanged. The outer
    // `buildPartialBlock` then adds it to the pipe.

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
    // All branches must give the same Swift type: the same stage type, with the same Input,
    // Output and Failure. A `Map` in one branch and a `Filter` in another does not compile.

    @inlinable
    public static func buildEither<C>(first component: C) -> C { component }
    @inlinable
    public static func buildEither<C>(second component: C) -> C { component }

    // MARK: - Optional composition (`if` without `else`)
    //
    // `buildOptional` puts the body's stage in an `OptionalStage<St>`. The overloads below add
    // the stage when it is there, and return the pipe unchanged when it is not. So only stages
    // that keep the types the same are allowed.

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

    // `for` loops are not supported on purpose. `buildArray` would need
    // `[some PipePolyStage<I, O>]`, and the compiler cannot infer that opaque type when the
    // stages come from functions that return `some Protocol`. Instead, combine the loop's work
    // into one stage outside the builder, for example `Map(transforms.reduce(id, compose))`.
}

/// A stage that may be missing: the body of an `if` without `else`.
///
/// It works only with stages that keep the element types the same.
public struct OptionalStage<St>: Sendable where St: Sendable {
    @usableFromInline let stage: St?

    @inlinable
    init(stage: St?) {
        self.stage = stage
    }
}

/// Passes on each `Result` input of an open pipe as a success or a failure.
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
    /// Creates a pipe from a source and stages.
    @inlinable
    init(@PipeBuilder _ build: () -> Pipe<Success, Failure>) {
        self = build()
    }
}

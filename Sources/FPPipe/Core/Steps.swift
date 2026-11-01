/// One stage's work on a single element: the element it emits for `input`, or `nil`
/// to drop `input`. A stage that never drops returns `.some`, which the iterator's
/// loop folds away once inlined.
///
/// Steps are structs the iterators below are generic over, so a step's body inlines
/// into the specialized iterator and the only call it leaves is the user's closure.
@usableFromInline
protocol PipeStep<Input, Output>: Sendable {
    associatedtype Input
    associatedtype Output

    func callAsFunction(_ input: Input) -> Output?
}

/// `PipeStep` for a stage whose work is asynchronous. The step runs on the isolation of
/// the task iterating the pipeline.
@usableFromInline
protocol AsyncPipeStep<Input, Output>: Sendable {
    associatedtype Input
    associatedtype Output

    func callAsFunction(_ input: Input) async -> Output?
}

/// Applies a synchronous step to each element of `Base`.
@usableFromInline
struct StepIterator<Base: AsyncIteratorProtocol, Step: PipeStep>: AsyncIteratorProtocol
where Base.Failure == Never, Step.Input == Base.Element {
    @usableFromInline var base: Base
    @usableFromInline let step: Step

    @inlinable
    init(_ base: Base, step: Step) {
        self.base = base
        self.step = step
    }

    @inlinable
    mutating func next() async -> Step.Output? {
        await next(isolation: #isolation)
    }

    @inlinable
    mutating func next(isolation actor: isolated (any Actor)?) async -> Step.Output? {
        while let element = await base.next(isolation: actor) {
            if let output = step(element) { return output }
        }
        return nil
    }
}

/// Applies an asynchronous step to each element of `Base`, one element at a time.
@usableFromInline
struct AsyncStepIterator<Base: AsyncIteratorProtocol, Step: AsyncPipeStep>: AsyncIteratorProtocol
where Base.Failure == Never, Step.Input == Base.Element {
    @usableFromInline var base: Base
    @usableFromInline let step: Step

    @inlinable
    init(_ base: Base, step: Step) {
        self.base = base
        self.step = step
    }

    @inlinable
    mutating func next() async -> Step.Output? {
        await next(isolation: #isolation)
    }

    @inlinable
    mutating func next(isolation actor: isolated (any Actor)?) async -> Step.Output? {
        while let element = await base.next(isolation: actor) {
            if let output = await step(element) { return output }
        }
        return nil
    }
}

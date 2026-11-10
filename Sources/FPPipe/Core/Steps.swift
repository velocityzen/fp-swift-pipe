/// One stage's work on one element: the element to pass on, or `nil` to drop it. Steps are
/// structs, so they inline into the iterators below.
@usableFromInline
protocol PipeStep<Input, Output>: Sendable {
    associatedtype Input
    associatedtype Output

    func callAsFunction(_ input: Input) -> Output?
}

/// A `PipeStep` with async work. It runs on the reader's isolation.
@usableFromInline
protocol AsyncPipeStep<Input, Output>: Sendable {
    associatedtype Input
    associatedtype Output

    func callAsFunction(_ input: Input) async -> Output?
}

/// Runs a sync step on each element of `Base`.
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

/// Runs an async step on each element of `Base`, one at a time.
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

/// Runs a sync step on each element of `Base`, as an `AsyncSequence`.
@usableFromInline
struct StepSequence<Base: AsyncSequence, Step: PipeStep>: AsyncSequence
where Base.Failure == Never, Step.Input == Base.Element {
    @usableFromInline typealias Element = Step.Output
    @usableFromInline typealias Failure = Never

    @usableFromInline let base: Base
    @usableFromInline let step: Step

    @inlinable
    init(_ base: Base, step: Step) {
        self.base = base
        self.step = step
    }

    @inlinable
    func makeAsyncIterator() -> StepIterator<Base.AsyncIterator, Step> {
        StepIterator(base.makeAsyncIterator(), step: step)
    }
}

extension StepSequence: Sendable where Base: Sendable {}

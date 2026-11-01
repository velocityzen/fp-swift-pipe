import FP

/// Expands each successful element into a sub-`AsyncSequence` of values, concatenated
/// in source order. Failures from upstream pass through unchanged; the inner sequence
/// is consumed to completion before moving to the next upstream element.
///
/// With a `bufferSize` of 1 the expansion runs in lockstep with the consumer: each
/// inner element is read when the consumer asks for it, in the consumer's task. With a
/// larger `bufferSize` a producer task runs ahead of the consumer by at most
/// `bufferSize` elements: past that it suspends until the consumer catches up, so a
/// fast inner sequence feeding a slow consumer stays bounded in memory.
///
/// The inner sequence must be non-throwing (`Inner.Failure == Never`) — to introduce
/// errors, use a `Result`-returning stage upstream or downstream.
@usableFromInline
struct FlatMapAsyncSequenceStage<Input: Sendable, Inner: AsyncSequence & Sendable>:
    PipePolyStage
where Inner.Element: Sendable, Inner.Failure == Never {
    @usableFromInline typealias Output = Inner.Element

    @usableFromInline let bufferSize: Int
    @usableFromInline let transform: @Sendable (Input) -> Inner

    @inlinable
    init(_ transform: @escaping @Sendable (Input) -> Inner, bufferSize: Int) {
        precondition(bufferSize >= 1, "bufferSize must be at least 1")
        self.bufferSize = bufferSize
        self.transform = transform
    }

    @inlinable
    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let transform = self.transform
        let expansion = Pipe<Output, F>.iterating {
            .init(FlatMapAsyncSequenceIterator(upstream.makeAsyncIterator(), transform: transform))
        }
        if bufferSize == 1 {
            return expansion
        }
        let bufferSize = self.bufferSize
        return .iterating { .init(expansion.readAhead(bufferSize).makeAsyncIterator()) }
    }
}

/// Lockstep expansion: reads the next upstream element only once the current inner
/// sequence is exhausted, and stops when the consuming task is cancelled. Above
/// `bufferSize: 1` it runs in the read-ahead task rather than the consumer's.
@usableFromInline
struct FlatMapAsyncSequenceIterator<
    Base: AsyncIteratorProtocol,
    Input,
    Inner: AsyncSequence,
    F: Error,
>: AsyncIteratorProtocol
where Base.Element == Result<Input, F>, Base.Failure == Never, Inner.Failure == Never {
    @usableFromInline var base: Base
    @usableFromInline let transform: @Sendable (Input) -> Inner
    @usableFromInline var inner: Inner.AsyncIterator?

    @inlinable
    init(_ base: Base, transform: @escaping @Sendable (Input) -> Inner) {
        self.base = base
        self.transform = transform
    }

    @inlinable
    mutating func next() async -> Result<Inner.Element, F>? {
        await next(isolation: #isolation)
    }

    @inlinable
    mutating func next(
        isolation actor: isolated (any Actor)?
    ) async -> Result<Inner.Element, F>? {
        while !Task.isCancelled {
            if inner != nil {
                if let value = await inner!.next(isolation: actor) { return .success(value) }
                inner = nil
            }
            switch await base.next(isolation: actor) {
                case nil: return nil
                case .failure(let error)?: return .failure(error)
                case .success(let value)?: inner = transform(value).makeAsyncIterator()
            }
        }
        return nil
    }
}

/// DSL: `FlatMapAsyncSequence { (url: URL) in fetchPages(for: url) }`.
///
/// Expansion is backpressured: at most `bufferSize` inner elements are buffered ahead
/// of the consumer; past that the expansion suspends until the consumer catches up.
/// With the default of 1 the expansion stays in lockstep with the consumer, running in
/// the consumer's task; raise `bufferSize` to let a producer task read further ahead of
/// a bursty consumer. Breaking out of iteration or cancelling the consumer stops the
/// expansion promptly.
@inlinable
public func FlatMapAsyncSequence<Input: Sendable, Inner: AsyncSequence & Sendable>(
    bufferSize: Int = 1,
    _ transform: @escaping @Sendable (Input) -> Inner,
) -> some PipePolyStage<Input, Inner.Element>
where Inner.Element: Sendable, Inner.Failure == Never {
    FlatMapAsyncSequenceStage(transform, bufferSize: bufferSize)
}

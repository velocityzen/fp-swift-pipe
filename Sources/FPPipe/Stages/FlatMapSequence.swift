/// Expands each successful element into a sub-`Sequence` of values, concatenated
/// in source order. Failures from upstream pass through unchanged.
///
/// With a `bufferSize` of 1 the expansion runs in lockstep with the consumer: each
/// inner element is produced when the consumer asks for it, in the consumer's task.
/// With a larger `bufferSize` a producer task runs ahead of the consumer by at most
/// `bufferSize` elements: past that it suspends until the consumer catches up, so a
/// huge inner sequence never lands in memory at once.
///
/// For async inner sequences, use `FlatMapAsyncSequence`.
@usableFromInline
struct FlatMapSequenceStage<Input: Sendable, Inner: Sequence & Sendable>: PipePolyStage
where Inner.Element: Sendable {
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
        if bufferSize == 1 {
            return .iterating {
                .init(FlatMapSequenceIterator(upstream.makeAsyncIterator(), transform: transform))
            }
        }
        let bufferSize = self.bufferSize
        return .iterating {
            // The gate bounds buffer occupancy to `bufferSize` — the producer acquires a
            // slot before every yield and the consumer releases one per element taken.
            // `.unbounded` is therefore safe: the policy only needs to be lossless, never
            // to bound.
            let gate = BackpressureGate(capacity: bufferSize)
            let stream = AsyncStream<Result<Output, F>>(bufferingPolicy: .unbounded) {
                continuation in
                let task = Task {
                    for await element in upstream {
                        switch element {
                            case .failure(let error):
                                await gate.acquire()
                                if !Task.isCancelled {
                                    continuation.yield(.failure(error))
                                }
                            case .success(let value):
                                for innerValue in transform(value) {
                                    await gate.acquire()
                                    if Task.isCancelled { break }
                                    continuation.yield(.success(innerValue))
                                }
                        }
                        if Task.isCancelled { break }
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
            return .init(StepIterator(stream.makeAsyncIterator(), step: ReleaseStep(gate)))
        }
    }
}

/// Lockstep expansion: reads the next upstream element only once the current inner
/// sequence is exhausted, and stops when the consuming task is cancelled.
@usableFromInline
struct FlatMapSequenceIterator<Base: AsyncIteratorProtocol, Input, Inner: Sequence, F: Error>:
    AsyncIteratorProtocol
where Base.Element == Result<Input, F>, Base.Failure == Never {
    @usableFromInline var base: Base
    @usableFromInline let transform: @Sendable (Input) -> Inner
    @usableFromInline var inner: Inner.Iterator?

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
                if let value = inner!.next() { return .success(value) }
                inner = nil
            }
            switch await base.next(isolation: actor) {
                case nil: return nil
                case .failure(let error)?: return .failure(error)
                case .success(let value)?: inner = transform(value).makeIterator()
            }
        }
        return nil
    }
}

/// DSL: `FlatMapSequence { (n: Int) in 0..<n }`.
///
/// Expansion is backpressured: at most `bufferSize` inner elements are buffered ahead
/// of the consumer; past that the expansion suspends until the consumer catches up, so
/// a million-element inner `Range` never sits in memory at once. With the default of 1
/// the expansion stays in lockstep with the consumer, running in the consumer's task;
/// raise `bufferSize` to let a producer task read further ahead of a bursty consumer.
/// Breaking out of iteration or cancelling the consumer stops the expansion promptly.
@inlinable
public func FlatMapSequence<Input: Sendable, Inner: Sequence & Sendable>(
    bufferSize: Int = 1,
    _ transform: @escaping @Sendable (Input) -> Inner,
) -> some PipePolyStage<Input, Inner.Element> where Inner.Element: Sendable {
    FlatMapSequenceStage(transform, bufferSize: bufferSize)
}

import FP

/// Expands each successful element into a sub-`AsyncSequence` of values, concatenated
/// in source order. Failures from upstream pass through unchanged; the inner sequence
/// is consumed to completion before moving to the next upstream element.
///
/// The expansion runs ahead of the consumer by at most `bufferSize` elements: past
/// that the producer suspends until the consumer catches up, so a fast inner sequence
/// feeding a slow consumer stays bounded in memory.
///
/// The inner sequence must be non-throwing (`Inner.Failure == Never`) — to introduce
/// errors, use a `Result`-returning stage upstream or downstream.
struct FlatMapAsyncSequenceStage<Input: Sendable, Inner: AsyncSequence & Sendable>:
    PipePolyStage
where Inner.Element: Sendable, Inner.Failure == Never {
    typealias Output = Inner.Element

    private let bufferSize: Int
    private let transform: @Sendable (Input) -> Inner

    init(_ transform: @escaping @Sendable (Input) -> Inner, bufferSize: Int) {
        self.bufferSize = bufferSize
        self.transform = transform
    }

    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let transform = self.transform
        let bufferSize = self.bufferSize
        return .erased {
            let source = upstream.upstream()
            // The gate bounds buffer occupancy to `bufferSize` — see FlatMapSequence.
            // `.unbounded` only needs to be lossless, never to bound.
            let gate = BackpressureGate(capacity: bufferSize)
            let stream = AsyncStream<Result<Output, F>>(bufferingPolicy: .unbounded) {
                continuation in
                let task = Task {
                    for await element in source {
                        switch element {
                            case .failure(let error):
                                await gate.acquire()
                                if !Task.isCancelled {
                                    continuation.failure(error)
                                }
                            case .success(let value):
                                for await innerValue in transform(value) {
                                    await gate.acquire()
                                    if Task.isCancelled { break }
                                    continuation.success(innerValue)
                                }
                        }
                        if Task.isCancelled { break }
                    }
                    continuation.finish()
                }
                continuation.onTermination = { _ in task.cancel() }
            }
            return AnyAsyncSequence(
                stream.map { (element: Result<Output, F>) -> Result<Output, F> in
                    gate.release()
                    return element
                },
            )
        }
    }
}

/// DSL: `FlatMapAsyncSequence { (url: URL) in fetchPages(for: url) }`.
///
/// Expansion is backpressured: at most `bufferSize` inner elements are buffered ahead
/// of the consumer; past that the expansion suspends until the consumer catches up.
/// With the default of 1 the expansion stays in lockstep with the consumer; raise
/// `bufferSize` to let it read further ahead of a bursty consumer. Breaking out of
/// iteration cancels the expansion promptly, parked or not.
public func FlatMapAsyncSequence<Input: Sendable, Inner: AsyncSequence & Sendable>(
    bufferSize: Int = 1,
    _ transform: @escaping @Sendable (Input) -> Inner,
) -> some PipePolyStage<Input, Inner.Element>
where Inner.Element: Sendable, Inner.Failure == Never {
    FlatMapAsyncSequenceStage(transform, bufferSize: bufferSize)
}

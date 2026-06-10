import FP

/// Expands each successful element into a sub-`Sequence` of values, concatenated
/// in source order. Failures from upstream pass through unchanged.
///
/// The expansion runs ahead of the consumer by at most `bufferSize` elements: past
/// that the producer suspends until the consumer catches up, so a huge inner
/// sequence never lands in memory at once.
///
/// For async inner sequences, use `FlatMapAsyncSequence`.
struct FlatMapSequenceStage<Input: Sendable, Inner: Sequence & Sendable>: PipePolyStage
where Inner.Element: Sendable {
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
            // The gate bounds buffer occupancy to `bufferSize` — the producer acquires a
            // slot before every yield and the consumer-side `map` below releases one per
            // element taken. `.unbounded` is therefore safe: the policy only needs to be
            // lossless, never to bound.
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
                                for innerValue in transform(value) {
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

/// DSL: `FlatMapSequence { (n: Int) in 0..<n }`.
///
/// Expansion is backpressured: at most `bufferSize` inner elements are buffered ahead
/// of the consumer; past that the expansion suspends until the consumer catches up, so
/// a million-element inner `Range` never sits in memory at once. Raise `bufferSize` to
/// let the expansion read further ahead of a bursty consumer. Breaking out of iteration
/// cancels the expansion promptly, parked or not.
public func FlatMapSequence<Input: Sendable, Inner: Sequence & Sendable>(
    bufferSize: Int = 16,
    _ transform: @escaping @Sendable (Input) -> Inner,
) -> some PipePolyStage<Input, Inner.Element> where Inner.Element: Sendable {
    FlatMapSequenceStage(transform, bufferSize: bufferSize)
}

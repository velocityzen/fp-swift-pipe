import FP

/// Expands each successful element into a sub-`Sequence` of values, concatenated
/// in source order. Failures from upstream pass through unchanged; the inner sequence
/// is consumed eagerly per upstream element.
///
/// For async inner sequences, use `FlatMapAsyncSequence`.
struct FlatMapSequenceStage<Input: Sendable, Inner: Sequence & Sendable>: PipePolyStage
where Inner.Element: Sendable {
    typealias Output = Inner.Element

    private let transform: @Sendable (Input) -> Inner

    init(_ transform: @escaping @Sendable (Input) -> Inner) {
        self.transform = transform
    }

    func attach<F: Error & Sendable>(_ upstream: Pipe<Input, F>) -> Pipe<Output, F> {
        let transform = self.transform
        return .erased {
            let source = upstream.upstream()
            return AnyAsyncSequence(
                // `.unbounded`: AsyncStream has no native backpressure. A single upstream
                // element whose inner sequence is huge (e.g. a million-element Range) will
                // queue every inner element before the next upstream element is processed.
                // The inner-loop cancellation check below at least lets the consumer abort
                // mid-expansion.
                AsyncStream<Result<Output, F>>(bufferingPolicy: .unbounded) { continuation in
                    let task = Task {
                        for await element in source {
                            switch element {
                                case .failure(let error):
                                    continuation.failure(error)
                                case .success(let value):
                                    for innerValue in transform(value) {
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
            )
        }
    }
}

/// DSL: `FlatMapSequence { (n: Int) in 0..<n }`.
public func FlatMapSequence<Input: Sendable, Inner: Sequence & Sendable>(
    _ transform: @escaping @Sendable (Input) -> Inner,
) -> some PipePolyStage<Input, Inner.Element> where Inner.Element: Sendable {
    FlatMapSequenceStage(transform)
}

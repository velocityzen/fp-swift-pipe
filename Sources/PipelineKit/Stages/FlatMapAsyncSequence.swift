import FP

/// Expands each successful element into a sub-`AsyncSequence` of values, concatenated
/// in source order. Failures from upstream pass through unchanged; the inner sequence
/// is consumed to completion before moving to the next upstream element.
///
/// The inner sequence must be non-throwing (`Inner.Failure == Never`) — to introduce
/// errors, use a `Result`-returning stage upstream or downstream.
struct FlatMapAsyncSequenceStage<Input: Sendable, Inner: AsyncSequence & Sendable>:
    PipePolyStage
where Inner.Element: Sendable, Inner.Failure == Never {
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
                AsyncStream<Result<Output, F>> { continuation in
                    let task = Task {
                        for await element in source {
                            switch element {
                                case .failure(let error):
                                    continuation.failure(error)
                                case .success(let value):
                                    for await innerValue in transform(value) {
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

/// DSL: `FlatMapAsyncSequence { (url: URL) in fetchPages(for: url) }`.
public func FlatMapAsyncSequence<Input: Sendable, Inner: AsyncSequence & Sendable>(
    _ transform: @escaping @Sendable (Input) -> Inner,
) -> some PipePolyStage<Input, Inner.Element>
where Inner.Element: Sendable, Inner.Failure == Never {
    FlatMapAsyncSequenceStage(transform)
}

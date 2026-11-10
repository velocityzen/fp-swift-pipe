import FP

// With `concurrency` above 1, the `Async*` stages run on fp-swift's `compactMapAsyncOrdered` or
// `compactMapAsyncUnordered`. The stage's `ordered` option picks one.

extension AsyncSequence where Self: Sendable, Failure == Never, Element: Sendable {
    /// Maps elements on up to `concurrency` tasks. Results come out in source order when
    /// `ordered` is true, and in the order they finish when it is false. `bufferLimit` caps the
    /// results that wait for an earlier, slower element, so it only matters when `ordered` is true.
    @inlinable
    func compactMapAsyncIterator<T: Sendable>(
        concurrency: Int,
        ordered: Bool,
        bufferLimit: Int,
        passthrough: (@Sendable (Element) -> T?)? = nil,
        _ transform: @concurrent @Sendable @escaping (Element) async -> T?,
    ) -> AnyAsyncSequence<T>.Iterator {
        if ordered {
            return .init(
                compactMapAsyncOrdered(
                    concurrency: concurrency,
                    bufferLimit: bufferLimit,
                    passthrough: passthrough,
                    transform,
                )
                .makeAsyncIterator()
            )
        }
        return .init(
            compactMapAsyncUnordered(
                concurrency: concurrency,
                passthrough: passthrough,
                transform,
            )
            .makeAsyncIterator()
        )
    }
}

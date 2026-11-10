import FP

extension AsyncSequence where Self: Sendable, Failure == Never, Element: Sendable {
    /// Reads the sequence in its own task, at most `count` elements ahead of the reader.
    ///
    /// It sends every element to the `passthrough` of `compactMapAsyncUnordered`, so the order
    /// stays the same. It stops when the reader stops.
    @inlinable
    func readAhead(_ count: Int) -> UnorderedMapSequence<Self, Element> {
        compactMapAsyncUnordered(concurrency: count, passthrough: { $0 }, { $0 })
    }
}

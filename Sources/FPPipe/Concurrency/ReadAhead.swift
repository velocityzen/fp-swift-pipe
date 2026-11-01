import FP

extension AsyncSequence where Self: Sendable, Failure == Never, Element: Sendable {
    /// Iterates the sequence in a task of its own, at most `count` elements ahead of
    /// the consumer: the fan-out stages above `bufferSize: 1`.
    ///
    /// fp-swift's `compactMapAsyncUnordered` reads its source in a task of its own and
    /// admits an element only while fewer than `concurrency` wait to be consumed. With
    /// every element passed through, no transform runs and elements come out in the order
    /// they were read, so it is a bounded read-ahead buffer. Cancelling the consumer, or
    /// dropping its iterator, stops the reading task, parked or not.
    @inlinable
    func readAhead(_ count: Int) -> UnorderedMapSequence<Self, Element> {
        compactMapAsyncUnordered(concurrency: count, passthrough: { $0 }, { $0 })
    }
}

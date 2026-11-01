@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable { case bad }

/// Counts how many inner elements the transform actually vends — lets tests assert
/// that expansion is bounded by backpressure rather than running to completion.
private final class Counter: Sendable {
    private let value = Mutex<Int>(0)
    func increment() { value.withLock { $0 += 1 } }
    var current: Int { value.withLock { $0 } }
}

/// `0..<upper` that counts each element as it is produced.
private struct CountingRange: Sequence, Sendable {
    let upper: Int
    let counter: Counter

    func makeIterator() -> AnyIterator<Int> {
        var n = 0
        let upper = self.upper
        let counter = self.counter
        return AnyIterator {
            guard n < upper else { return nil }
            counter.increment()
            defer { n += 1 }
            return n
        }
    }
}

@Test
func flatMapSequenceExpandsEachSuccess() async {
    let pipe = Pipe<Int, Never> {
        From([2, 3])
        FlatMapSequence { (n: Int) in 0..<n }
    }
    let result = await pipe.toResult()
    #expect(result == .success([0, 1, 0, 1, 2]))
}

@Test
func flatMapSequenceWithEmptyInnerEmitsNothing() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        FlatMapSequence { (_: Int) in [Int]() }
    }
    let result = await pipe.toResult()
    #expect(result == .success([]))
}

@Test
func flatMapSequenceForwardsUpstreamFailures() async {
    let pipe = Pipe<Int, E> {
        From([2, -1, 2])
        FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
        FlatMapSequence { (n: Int) in 0..<n }
    }

    var seen: [Result<Int, E>] = []
    for await x in pipe {
        seen.append(x)
    }
    // The failure passes through in source position; the transform never sees it.
    #expect(seen == [.success(0), .success(1), .failure(.bad), .success(0), .success(1)])
}

@Test
func flatMapSequenceLargeInnerExpansionStopsOnEarlyBreak() async {
    // One element fans out into 1M inner elements; the consumer takes 5 and stops.
    // Backpressure parks the producer once the buffer fills, and stream teardown
    // unblocks and cancels it — the sink returns with just the prefix.
    let pipe = Pipe<Int, Never> {
        From([1_000_000])
        FlatMapSequence { (n: Int) in 0..<n }
        Take(5)
    }
    let result = await pipe.toResult()
    #expect(result == .success([0, 1, 2, 3, 4]))
}

@Test
func flatMapSequenceExpansionIsBoundedByBufferSize() async {
    let counter = Counter()
    let pipe = Pipe<Int, Never> {
        From([1_000])
        FlatMapSequence(bufferSize: 8) { (n: Int) in CountingRange(upper: n, counter: counter) }
        Take(5)
    }

    let result = await pipe.toResult()
    #expect(result == .success([0, 1, 2, 3, 4]))
    // Invariant, not a race: the producer waits for a free slot before it expands the
    // next element, so it never runs more than `bufferSize` ahead of the 5 consumed
    // elements. The remaining ~987 elements are provably never produced.
    #expect(counter.current <= 5 + 8)
}

@Test
func flatMapSequenceWithBufferSizeOneDeliversEverythingInOrder() async {
    // Lockstep expansion of a large inner sequence: every element arrives, in order.
    let pipe = Pipe<Int, Never> {
        From([10_000])
        FlatMapSequence(bufferSize: 1) { (n: Int) in 0..<n }
    }
    let result = await pipe.toResult()
    #expect(result == .success(Array(0..<10_000)))
}

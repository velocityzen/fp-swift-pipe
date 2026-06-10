@testable import FPPipe
import Synchronization
import Testing

private enum AppError: Error, Equatable { case bad }

/// Counts how many inner elements the transform actually vends — lets tests assert
/// that expansion is bounded by backpressure rather than running to completion.
private final class Counter: Sendable {
    private let value = Mutex<Int>(0)
    func increment() { value.withLock { $0 += 1 } }
    var current: Int { value.withLock { $0 } }
}

/// Async `0..<upper` that counts each element as it is produced.
private struct CountingAsyncRange: AsyncSequence, Sendable {
    typealias Element = Int

    let upper: Int
    let counter: Counter

    struct AsyncIterator: AsyncIteratorProtocol {
        var n = 0
        let upper: Int
        let counter: Counter

        mutating func next() async -> Int? {
            guard n < upper else { return nil }
            counter.increment()
            defer { n += 1 }
            return n
        }
    }

    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(upper: upper, counter: counter)
    }
}

@Test
func flatMapAsyncSequenceFansOutEachSuccess() async {
    let pipe = Pipe<Int, Never> {
        From([2, 3])
        FlatMapAsyncSequence { (n: Int) in
            AsyncStream<Int> { continuation in
                for i in 0..<n {
                    continuation.yield(i)
                }
                continuation.finish()
            }
        }
    }

    let result = await pipe.toResult()
    #expect(result == .success([0, 1, 0, 1, 2]))
}

@Test
func flatMapAsyncSequencePassesFailuresThrough() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 2 ? .failure(.bad) : .success(n)
        }
        FlatMapAsyncSequence { (n: Int) in
            AsyncStream<Int> { continuation in
                continuation.yield(n * 10)
                continuation.yield(n * 100)
                continuation.finish()
            }
        }
    }

    var observed: [Result<Int, AppError>] = []
    for await element in pipe {
        observed.append(element)
    }

    #expect(
        observed == [
            .success(10), .success(100),
            .failure(.bad),
            .success(30), .success(300),
        ]
    )
}

@Test
func flatMapAsyncSequenceExpansionIsBoundedByBufferSize() async {
    let counter = Counter()
    let pipe = Pipe<Int, Never> {
        From([1_000])
        FlatMapAsyncSequence(bufferSize: 8) { (n: Int) in
            CountingAsyncRange(upper: n, counter: counter)
        }
        Take(5)
    }

    let result = await pipe.toResult()
    #expect(result == .success([0, 1, 2, 3, 4]))
    // Invariant, not a race: the producer can never run more than `bufferSize` slots
    // ahead of the 5 consumed elements, plus the one element vended while awaiting its
    // slot. The remaining ~986 elements are provably never produced.
    #expect(counter.current <= 5 + 8 + 1)
}

@Test
func flatMapAsyncSequenceWithBufferSizeOneDeliversEverythingInOrder() async {
    // Rendezvous-sized buffer over a large expansion: every element round-trips
    // through producer suspension and consumer release without loss or reordering.
    let pipe = Pipe<Int, Never> {
        From([2_000])
        FlatMapAsyncSequence(bufferSize: 1) { (n: Int) in
            CountingAsyncRange(upper: n, counter: Counter())
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success(Array(0..<2_000)))
}

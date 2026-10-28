@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable, Sendable { case bad }

// MARK: - Unordered Async* stages stay bounded under a slow consumer
//
// Pattern: instant transforms over a large source, consumed slowly. Without backpressure the
// producer races through the whole source while the consumer sleeps; with it, a slot is only
// freed when the consumer takes a result, so `started` never exceeds `taken + concurrency`.

private let concurrency = 4

private func expectBounded<F: Error & Sendable>(
    _ pipe: Pipe<Int, F>,
    started: borrowing Mutex<Int>,
    sourceLocation: SourceLocation = #_sourceLocation,
) async {
    var taken = 0
    for await _ in pipe {
        taken += 1
        // Give a runaway producer every chance to show itself.
        try? await Task.sleep(nanoseconds: 10_000_000)
        let total = started.withLock { $0 }
        #expect(
            total <= taken + concurrency,
            "started \(total) after taking \(taken) — producer ran ahead of the consumer",
            sourceLocation: sourceLocation,
        )
        if taken == 5 { break }
    }
}

@Test
func asyncMapConcurrentIsBoundedBySlowConsumer() async {
    let started = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<100_000)
        AsyncMap(concurrency: concurrency) { (n: Int) async -> Int in
            started.withLock { $0 += 1 }
            return n
        }
    }
    await expectBounded(pipe, started: started)
}

@Test
func asyncFlatMapConcurrentIsBoundedBySlowConsumer() async {
    let started = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From(0..<100_000)
        AsyncFlatMap(concurrency: concurrency) { (n: Int) async -> Result<Int, E> in
            started.withLock { $0 += 1 }
            return .success(n)
        }
    }
    await expectBounded(pipe, started: started)
}

@Test
func asyncFilterConcurrentIsBoundedBySlowConsumer() async {
    let started = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<100_000)
        AsyncFilter(concurrency: concurrency) { (_: Int) async -> Bool in
            started.withLock { $0 += 1 }
            return true
        }
    }
    await expectBounded(pipe, started: started)
}

@Test
func asyncCompactMapConcurrentIsBoundedBySlowConsumer() async {
    let started = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<100_000)
        AsyncCompactMap(concurrency: concurrency) { (n: Int) async -> Int? in
            started.withLock { $0 += 1 }
            return n
        }
    }
    await expectBounded(pipe, started: started)
}

@Test
func droppedElementsFreeTheirSlots() async {
    // Every other element is dropped by the transform; dropped elements must give their slot
    // back immediately or the stage would stall once `concurrency` of them were in flight.
    let pipe = Pipe<Int, Never> {
        From(0..<1_000)
        AsyncCompactMap(concurrency: 2) { (n: Int) async -> Int? in
            n.isMultiple(of: 2) ? nil : n
        }
    }
    let result = await pipe.toResult()
    #expect(result.map { $0.sorted() } == .success(Array(stride(from: 1, to: 1_000, by: 2))))
}

@Test
func passthroughElementsSkipTheTransform() async {
    let calls = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From([1, -1, 2, -2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
        AsyncMap(concurrency: 3) { (n: Int) async -> Int in
            calls.withLock { $0 += 1 }
            return n * 10
        }
    }
    var successes: [Int] = []
    var failures = 0
    for await element in pipe {
        switch element {
            case .success(let value): successes.append(value)
            case .failure: failures += 1
        }
    }
    #expect(successes.sorted() == [10, 20, 30])
    #expect(failures == 2)
    #expect(calls.withLock { $0 } == 3)
}

// MARK: - Unordered Async* stages emit without waiting on a slow source

@Test
func asyncMapConcurrentEmitsBeforeSourceFillsTheWindow() async {
    let (source, continuation) = AsyncStream.makeStream(of: Int.self)
    let pipe = Pipe<Int, Never> {
        From(source)
        AsyncMap(concurrency: 10) { (n: Int) async -> Int in n * 2 }
    }
    let first = Mutex<Int?>(nil)
    let consumer = Task {
        for await element in pipe {
            first.withLock { $0 = element.get() }
            break
        }
    }

    // One element only: the window (10) never fills, so a stage that primes the whole
    // window or gates emission on the next source element would never emit.
    continuation.yield(21)
    var waited = 0
    while first.withLock({ $0 }) == nil, waited < 200 {
        try? await Task.sleep(nanoseconds: 10_000_000)
        waited += 1
    }
    #expect(first.withLock { $0 } == 42, "no result while the source was idle")

    continuation.finish()
    consumer.cancel()
    await consumer.value
}

// MARK: - FromAsync reads its inner sequence lazily

private final class ReadCounter: Sendable {
    let reads = Mutex<Int>(0)
}

private struct CountingSequence: Sequence, Sendable {
    let counter: ReadCounter
    let limit: Int

    func makeIterator() -> AnyIterator<Int> {
        var next = 0
        let counter = counter
        let limit = limit
        return AnyIterator {
            guard next < limit else { return nil }
            counter.reads.withLock { $0 += 1 }
            defer { next += 1 }
            return next
        }
    }
}

@Test
func fromAsyncSyncSequenceReadsOnlyWhatIsConsumed() async {
    let counter = ReadCounter()
    let pipe = Pipe<Int, Never> {
        FromAsync { () async -> CountingSequence in
            CountingSequence(counter: counter, limit: 1_000_000)
        }
    }

    var taken = 0
    for await _ in pipe {
        taken += 1
        try? await Task.sleep(nanoseconds: 5_000_000)
        let reads = counter.reads.withLock { $0 }
        #expect(reads == taken, "read \(reads) after taking \(taken) — source ran ahead")
        if taken == 3 { break }
    }
}

@Test
func fromAsyncAwaitsMakeOnlyOnceIterationStarts() async {
    let calls = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        FromAsync { () async -> [Int] in
            calls.withLock { $0 += 1 }
            return [1, 2]
        }
    }
    var iterator = pipe.makeAsyncIterator()
    #expect(calls.withLock { $0 } == 0)
    _ = await iterator.next()
    _ = await iterator.next()
    #expect(calls.withLock { $0 } == 1)
}

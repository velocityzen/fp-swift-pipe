@testable import FPPipe
import Synchronization
import Testing

// The fan-out stages above `bufferSize: 1`: a task of their own expands the elements,
// at most `bufferSize` ahead of the consumer.

private enum E: Error, Equatable, Sendable { case bad }

/// Counts the inner elements a fan-out has produced.
private final class Produced: Sendable {
    private let value = Mutex(0)
    func increment() { value.withLock { $0 += 1 } }
    var current: Int { value.withLock { $0 } }
}

/// `lower..<upper`, counting each element as it is produced.
private struct CountingInner: Sequence, Sendable {
    let lower: Int
    let upper: Int
    let produced: Produced

    func makeIterator() -> AnyIterator<Int> {
        var next = lower
        let upper = upper
        let produced = produced
        return AnyIterator {
            guard next < upper else { return nil }
            produced.increment()
            defer { next += 1 }
            return next
        }
    }
}

/// Async `lower..<upper`, counting each element as it is produced; `pause` sleeps
/// before each one, which lets a test catch the producer inside the inner sequence.
private struct CountingAsyncInner: AsyncSequence, Sendable {
    let lower: Int
    let upper: Int
    let produced: Produced
    var pause: Duration?

    struct AsyncIterator: AsyncIteratorProtocol {
        var next: Int
        let upper: Int
        let produced: Produced
        let pause: Duration?

        mutating func next() async -> Int? {
            guard next < upper else { return nil }
            if let pause { try? await Task.sleep(for: pause) }
            produced.increment()
            defer { next += 1 }
            return next
        }
    }

    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(next: lower, upper: upper, produced: produced, pause: pause)
    }
}

/// The elements of `0..<200` that fail on multiples of 7, each success `n` expanded to
/// `n..<(n + 3)`.
private let expected: [Result<Int, E>] = (0..<200).flatMap { (n: Int) -> [Result<Int, E>] in
    n.isMultiple(of: 7) ? [.failure(.bad)] : (n..<(n + 3)).map { .success($0) }
}

private func failingMultiplesOfSeven(_ n: Int) -> Result<Int, E> {
    n.isMultiple(of: 7) ? .failure(.bad) : .success(n)
}

/// Waits until `produced` stops growing, which it never does while a producer runs on
/// after the consumer is gone. Returns the count it settled at, or nil if it never did.
private func settled(_ produced: Produced) async -> Int? {
    var last = produced.current
    for _ in 0..<100 {
        try? await Task.sleep(for: .milliseconds(30))
        let now = produced.current
        if now == last { return now }
        last = now
    }
    return nil
}

// MARK: - Order

@Test(arguments: [2, 16, Int.max])
func bufferedFlatMapSequenceKeepsOrderAndFailures(bufferSize: Int) async {
    let pipe = Pipe<Int, E> {
        From(0..<200)
        FlatMap(failingMultiplesOfSeven)
        FlatMapSequence(bufferSize: bufferSize) { (n: Int) in n..<(n + 3) }
    }

    var observed: [Result<Int, E>] = []
    for await element in pipe {
        observed.append(element)
    }
    #expect(observed == expected)
}

@Test(arguments: [2, 16, Int.max])
func bufferedFlatMapAsyncSequenceKeepsOrderAndFailures(bufferSize: Int) async {
    let produced = Produced()
    let pipe = Pipe<Int, E> {
        From(0..<200)
        FlatMap(failingMultiplesOfSeven)
        FlatMapAsyncSequence(bufferSize: bufferSize) { (n: Int) in
            CountingAsyncInner(lower: n, upper: n + 3, produced: produced)
        }
    }

    var observed: [Result<Int, E>] = []
    for await element in pipe {
        observed.append(element)
    }
    #expect(observed == expected)
}

@Test
func bufferedFanOutIteratesAfresh() async {
    let pipe = Pipe<Int, Never> {
        From([3, 2])
        FlatMapSequence(bufferSize: 4) { (n: Int) in 0..<n }
    }

    #expect(await pipe.toResult() == .success([0, 1, 2, 0, 1]))
    #expect(await pipe.toResult() == .success([0, 1, 2, 0, 1]))
}

// MARK: - Backpressure

@Test
func bufferedFlatMapSequenceIsBoundedBySlowConsumer() async {
    let produced = Produced()
    let pipe = Pipe<Int, Never> {
        From([1_000])
        FlatMapSequence(bufferSize: 4) { (n: Int) in
            CountingInner(lower: 0, upper: n, produced: produced)
        }
    }

    var taken = 0
    for await _ in pipe {
        taken += 1
        // Give a runaway producer every chance to show itself.
        try? await Task.sleep(for: .milliseconds(10))
        #expect(
            produced.current <= taken + 4,
            "produced \(produced.current) after taking \(taken)",
        )
        if taken == 5 { break }
    }
}

@Test
func bufferedFlatMapAsyncSequenceIsBoundedBySlowConsumer() async {
    let produced = Produced()
    let pipe = Pipe<Int, Never> {
        From([1_000])
        FlatMapAsyncSequence(bufferSize: 4) { (n: Int) in
            CountingAsyncInner(lower: 0, upper: n, produced: produced)
        }
    }

    var taken = 0
    for await _ in pipe {
        taken += 1
        try? await Task.sleep(for: .milliseconds(10))
        #expect(
            produced.current <= taken + 4,
            "produced \(produced.current) after taking \(taken)",
        )
        if taken == 5 { break }
    }
}

// MARK: - Cancellation

/// An inner sequence that sleeps before every element, read ahead without a bound, so
/// the producer is busy inside it — not parked on a slot — when the consumer goes.
private func endlessFanOut(_ produced: Produced) -> Pipe<Int, Never> {
    Pipe<Int, Never> {
        From([Int.max])
        FlatMapAsyncSequence(bufferSize: .max) { (n: Int) in
            CountingAsyncInner(lower: 0, upper: n, produced: produced, pause: .milliseconds(1))
        }
    }
}

@Test
func cancellingTheConsumerStopsTheProducer() async {
    let produced = Produced()
    let pipe = endlessFanOut(produced)

    let consumer = Task {
        var taken = 0
        for await _ in pipe {
            taken += 1
            if taken == 3 {
                withUnsafeCurrentTask { $0?.cancel() }
            }
        }
        return taken
    }

    #expect(await consumer.value == 3)
    #expect(await settled(produced) != nil, "the producer ran on after the consumer was cancelled")
}

@Test
func droppingTheIteratorStopsTheProducer() async {
    let produced = Produced()
    let pipe = endlessFanOut(produced)

    // No cancellation: releasing the iterator is the only signal the producer gets.
    var taken = 0
    for await _ in pipe {
        taken += 1
        if taken == 3 { break }
    }

    #expect(taken == 3)
    #expect(await settled(produced) != nil, "the producer ran on after the iterator was dropped")
}

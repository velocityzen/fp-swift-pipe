import FP
@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable { case bad }

// MARK: - toResult

@Test
func toResultRunsEveryStage() async {
    let counter = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        Map { (n: Int) -> Int in
            counter.withLock { $0 += 1 }
            return n
        }
    }
    await pipe.toResult()
    #expect(counter.withLock { $0 } == 3)
}

@Test
func toResultOnEmptySourceYieldsEmptyArray() async {
    let pipe = Pipe<Int, Never> {
        From([Int]())
        Map { (n: Int) in n }
    }
    let result = await pipe.toResult()
    #expect(result == .success([]))
}

@Test
func toResultShortCircuitsAtFirstFailure() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in
            n == 2 ? .failure(.bad) : .success(n)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .failure(.bad))
}

// MARK: - toArray

@Test
func toArrayCollectsEveryElementAndDoesNotShortCircuit() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3, 4])
        FlatMap { (n: Int) -> Result<Int, E> in
            n.isMultiple(of: 2) ? .failure(.bad) : .success(n)
        }
    }

    let elements = await pipe.toArray()
    #expect(elements == [.success(1), .failure(.bad), .success(3), .failure(.bad)])
}

@Test
func toArrayOnEmptySourceIsEmpty() async {
    let pipe = Pipe<Int, E> {
        Empty(valueType: Int.self, failureType: E.self)
    }
    let elements = await pipe.toArray()
    #expect(elements.isEmpty)
}

// MARK: - reduce

@Test
func reduceSumsSuccess() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4])
        Map { (n: Int) in n }
    }
    let total = await pipe.reduce(0, +)
    #expect(total == .success(10))
}

@Test
func reduceThatIgnoresTheValueStillGivesAResult() async {
    // fp-swift's sink, not the standard library's `reduce`, which would give an `Int`
    // and fold the `Result`s themselves.
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in .success(n) }
    }
    let count = await pipe.reduce(0) { count, _ in count + 1 }
    #expect(count == .success(3))
}

@Test
func reduceStopsReadingAtTheFirstFailure() async {
    let reads = Mutex(0)
    let pipe = Pipe<Int, E> {
        From([1, 2, 3, 4])
        Tap { (_: Int) in reads.withLock { $0 += 1 } }
        FlatMap { (n: Int) -> Result<Int, E> in n == 2 ? .failure(.bad) : .success(n) }
    }
    #expect(await pipe.reduce(0, +) == .failure(.bad))
    #expect(reads.withLock { $0 } == 2)
}

@Test
func reduceIntoUpdatesOneValue() async {
    let pipe = Pipe<String, Never> { From(["a", "bb", "c"]) }
    let byLength = await pipe.reduce(into: [Int: [String]]()) { groups, word in
        groups[word.count, default: []].append(word)
    }
    #expect(byLength == .success([1: ["a", "c"], 2: ["bb"]]))
}

// MARK: - separate / last…

/// Gives `.success(1)`, `.failure(.bad)`, `.success(3)`, `.failure(.bad)`.
private func alternating() -> Pipe<Int, E> {
    Pipe<Int, E> {
        From([1, 2, 3, 4])
        FlatMap { (n: Int) -> Result<Int, E> in
            n.isMultiple(of: 2) ? .failure(.bad) : .success(n)
        }
    }
}

@Test
func separateSplitsEveryElement() async {
    let (successes, failures) = await alternating().separate()
    #expect(successes == [1, 3])
    #expect(failures == [.bad, .bad])
}

@Test
func lastSinksReadToTheEnd() async {
    let pipe = alternating()
    #expect(await pipe.last() == .failure(.bad))
    #expect(await pipe.lastSuccess() == 3)
    #expect(await pipe.lastError() == .bad)

    let empty = Pipe<Int, E> { Empty(valueType: Int.self, failureType: E.self) }
    #expect(await empty.last() == nil)
    #expect(await empty.lastSuccess() == nil)
    #expect(await empty.lastError() == nil)
}

@Test
func firstSuccessOrLastErrorPrefersASuccess() async {
    #expect(await alternating().firstSuccessOrLastError() == .success(1))

    let failing = Pipe<Int, E> {
        From([1, 2])
        FlatMap { (_: Int) -> Result<Int, E> in .failure(.bad) }
    }
    #expect(await failing.firstSuccessOrLastError() == .failure(.bad))

    let empty = Pipe<Int, E> { Empty(valueType: Int.self, failureType: E.self) }
    #expect(await empty.firstSuccessOrLastError() == nil)
}

// MARK: - first / firstSuccess / firstError

@Test
func firstReturnsLeadingElementRegardlessOfKind() async {
    let pipe = Pipe<Int, E> {
        From([1, 2])
        FlatMap { (_: Int) -> Result<Int, E> in .failure(.bad) }
    }
    let head = await pipe.first()
    #expect(head == .failure(.bad))
}

@Test
func firstSuccessSkipsLeadingFailures() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in
            n < 3 ? .failure(.bad) : .success(n * 10)
        }
    }
    let value = await pipe.firstSuccess()
    #expect(value == 30)
}

@Test
func firstSuccessIsNilWhenAllFailures() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap { (_: Int) -> Result<Int, E> in .failure(.bad) }
    }
    let value = await pipe.firstSuccess()
    #expect(value == nil)
}

@Test
func firstErrorSkipsLeadingSuccesses() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3, 4])
        FlatMap { (n: Int) -> Result<Int, E> in
            n < 3 ? .success(n) : .failure(.bad)
        }
    }
    let error = await pipe.firstError()
    #expect(error == .bad)
}

@Test
func firstErrorIsNilWhenAllSuccesses() async {
    let pipe = Pipe<Int, Never> { From([1, 2, 3]) }
    let error: Never? = await pipe.firstError()
    #expect(error == nil)
}

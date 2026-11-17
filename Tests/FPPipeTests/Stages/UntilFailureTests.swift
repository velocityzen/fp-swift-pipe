import FP
@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable { case bad(Int) }

/// Fails for negative numbers.
private func check(_ n: Int) -> Result<Int, E> {
    n < 0 ? .failure(.bad(n)) : .success(n)
}

@Test
func untilFailureKeepsTheElementsUpToAndIncludingTheFirstFailure() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, -3, 4, -5])
        FlatMap(check)
        UntilFailure()
    }

    let expected: [Result<Int, E>] = [.success(1), .success(2), .failure(.bad(-3))]
    #expect(await pipe.toArray() == expected)
    // It runs again, like every pipe.
    #expect(await pipe.toArray() == expected)
}

@Test
func untilFailurePassesEverythingWhenNothingFails() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap(check)
        UntilFailure()
    }
    let never = Pipe<Int, Never> {
        From([1, 2, 3])
        UntilFailure()
    }
    let empty = Pipe<Int, E> {
        Empty(valueType: Int.self, failureType: E.self)
        UntilFailure()
    }

    #expect(await pipe.toResult() == .success([1, 2, 3]))
    #expect(await never.toResult() == .success([1, 2, 3]))
    #expect(await empty.toArray().isEmpty)
}

@Test
func untilFailureStopsReadingAfterTheFailure() async {
    let reads = Mutex(0)
    let pipe = Pipe<Int, E> {
        From([1, -2, 3, 4])
        Tap { (_: Int) in reads.withLock { $0 += 1 } }
        FlatMap(check)
        UntilFailure()
    }

    var iterator = pipe.makeAsyncIterator()
    #expect(await iterator.next() == .success(1))
    #expect(await iterator.next() == .failure(.bad(-2)))
    #expect(await iterator.next() == nil)
    #expect(await iterator.next() == nil)
    #expect(reads.withLock { $0 } == 2)
}

@Test
func untilFailureEndsAnEndlessPipe() async {
    let pipe = Pipe<Int, E> {
        From(1...)
        FlatMap { (n: Int) in check(n == 4 ? -n : n) }
        UntilFailure()
    }

    #expect(await pipe.toArray() == [.success(1), .success(2), .success(3), .failure(.bad(-4))])
}

@Test
func untilFailureStopsAConcurrentStageBeforeIt() async {
    let started = Mutex(0)
    let pipe = Pipe<Int, E> {
        From(1...)
        AsyncFlatMap(concurrency: 4, ordered: true) { (n: Int) async -> Result<Int, E> in
            started.withLock { $0 += 1 }
            return check(n == 3 ? -n : n)
        }
        UntilFailure()
    }

    // The source never ends, so this returns only because the stage stops the pipe.
    #expect(await pipe.toArray() == [.success(1), .success(2), .failure(.bad(-3))])
    // Only the workers already running get past the failure: 4 to 8 of them start in all, by
    // measure. The bound leaves room for a slow machine and still fails a stage that reads on.
    #expect(started.withLock { $0 } <= 16)
}

@Test
func untilFailureComposesWithStagesAfterIt() async {
    let pipe = Pipe<String, Never> {
        From([1, 2, -3, 4])
        FlatMap(check)
        UntilFailure()
        Map { (n: Int) in "#\(n)" }
        GetOrElse { (error: E) in "\(error)" }
    }

    #expect(await pipe.toResult() == .success(["#1", "#2", "bad(-3)"]))
}

@Test
func untilFailureWorksInAnOpenPipeAndABranch() async {
    func make(_ stops: Bool) -> OpenPipe<Int, Int, E> {
        OpenPipe {
            From(Int.self)
            FlatMap(check)
            if stops {
                UntilFailure()
            }
        }
    }

    #expect(await make(true)([1, -2, 3]).toArray() == [.success(1), .failure(.bad(-2))])
    #expect(
        await make(false)([1, -2, 3]).toArray() == [.success(1), .failure(.bad(-2)), .success(3)]
    )
}

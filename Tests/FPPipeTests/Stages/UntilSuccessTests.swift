import FP
@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable { case notReady(Int) }

/// Succeeds for numbers of 3 and more.
private func check(_ n: Int) -> Result<Int, E> {
    n < 3 ? .failure(.notReady(n)) : .success(n)
}

@Test
func untilSuccessKeepsTheElementsUpToAndIncludingTheFirstSuccess() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3, 4, 1])
        FlatMap(check)
        UntilSuccess()
    }

    let expected: [Result<Int, E>] = [.failure(.notReady(1)), .failure(.notReady(2)), .success(3)]
    #expect(await pipe.toArray() == expected)
    // It runs again, like every pipe.
    #expect(await pipe.toArray() == expected)
}

@Test
func untilSuccessPassesEverythingWhenNothingSucceeds() async {
    let pipe = Pipe<Int, E> {
        From([0, 1, 2])
        FlatMap(check)
        UntilSuccess()
    }
    let empty = Pipe<Int, E> {
        Empty(valueType: Int.self, failureType: E.self)
        UntilSuccess()
    }

    #expect(
        await pipe.toArray() == [
            .failure(.notReady(0)), .failure(.notReady(1)), .failure(.notReady(2)),
        ]
    )
    #expect(await empty.toArray().isEmpty)
}

@Test
func untilSuccessStopsReadingAfterTheSuccess() async {
    let reads = Mutex(0)
    let pipe = Pipe<Int, E> {
        From([1, 3, 4, 5])
        Tap { (_: Int) in reads.withLock { $0 += 1 } }
        FlatMap(check)
        UntilSuccess()
    }

    var iterator = pipe.makeAsyncIterator()
    #expect(await iterator.next() == .failure(.notReady(1)))
    #expect(await iterator.next() == .success(3))
    #expect(await iterator.next() == nil)
    #expect(await iterator.next() == nil)
    #expect(reads.withLock { $0 } == 2)

    // A pipe that cannot fail ends after its first element.
    let never = Pipe<Int, Never> {
        From([1, 2, 3])
        UntilSuccess()
    }
    #expect(await never.toResult() == .success([1]))
}

@Test
func untilSuccessEndsAPoll() async {
    let calls = Mutex(0)
    let pipe = Pipe<Int, E> {
        FromResult(
            withRepeat(interval: .zero) {
                let call = calls.withLock { calls in
                    calls += 1
                    return calls
                }
                return check(call)
            }
        )
        UntilSuccess()
    }

    #expect(await pipe.toArray() == [.failure(.notReady(1)), .failure(.notReady(2)), .success(3)])
    #expect(calls.withLock { $0 } == 3)
}

@Test
func untilSuccessStopsAConcurrentStageBeforeIt() async {
    let started = Mutex(0)
    let pipe = Pipe<Int, E> {
        From(1...)
        AsyncFlatMap(concurrency: 4, ordered: true) { (n: Int) async -> Result<Int, E> in
            started.withLock { $0 += 1 }
            return check(n)
        }
        UntilSuccess()
    }

    // The source never ends, so this returns only because the stage stops the pipe.
    #expect(await pipe.toArray() == [.failure(.notReady(1)), .failure(.notReady(2)), .success(3)])
    // Only the workers already running get past the success, as with `UntilFailure`.
    #expect(started.withLock { $0 } <= 16)
}

@Test
func untilSuccessComposesWithStagesAfterIt() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4])
        FlatMap(check)
        UntilSuccess()
        GetOrElse { (_: E) in 0 }
        Map { (n: Int) in n * 10 }
    }

    #expect(await pipe.toResult() == .success([0, 0, 30]))
}

@Test
func untilSuccessWorksInAnOpenPipeAndABranch() async {
    func make(_ stops: Bool) -> OpenPipe<Int, Int, E> {
        OpenPipe {
            From(Int.self)
            FlatMap(check)
            if stops {
                UntilSuccess()
            }
        }
    }

    #expect(await make(true)([1, 3, 4]).toArray() == [.failure(.notReady(1)), .success(3)])
    #expect(
        await make(false)([1, 3, 4]).toArray() == [
            .failure(.notReady(1)), .success(3), .success(4),
        ]
    )
}

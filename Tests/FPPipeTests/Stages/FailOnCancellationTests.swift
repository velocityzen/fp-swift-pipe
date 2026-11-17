import FP
@testable import FPPipe
import Synchronization
import Testing

private enum SyncError: Error, Equatable {
    case bad
    case cancelled
}

/// Runs `read` in a task that is cancelled before it starts reading.
private func inCancelledTask<T: Sendable>(
    _ read: @escaping @Sendable () async -> T
) async -> T {
    await Task {
        withUnsafeCurrentTask { $0?.cancel() }
        return await read()
    }
    .value
}

@Test
func failOnCancellationPassesEveryElementThrough() async {
    let pipe = Pipe<Int, SyncError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, SyncError> in n == 2 ? .failure(.bad) : .success(n) }
        FailOnCancellation(SyncError.cancelled)
    }

    #expect(await pipe.toArray() == [.success(1), .failure(.bad), .success(3)])
    // It runs again, like every pipe.
    #expect(await pipe.toArray() == [.success(1), .failure(.bad), .success(3)])
}

@Test
func failOnCancellationGivesOneFailureInACancelledTask() async {
    let reads = Mutex(0)
    let pipe = Pipe<Int, SyncError> {
        From([1, 2, 3])
        Tap { (_: Int) in reads.withLock { $0 += 1 } }
        FailOnCancellation(SyncError.cancelled)
    }

    #expect(await inCancelledTask { await pipe.toArray() } == [.failure(.cancelled)])
    #expect(await inCancelledTask { await pipe.toResult() } == .failure(.cancelled))
    #expect(reads.withLock { $0 } == 0)
}

@Test
func failOnCancellationEndsAfterTheFailure() async {
    let reads = Mutex(0)
    let pipe = Pipe<Int, SyncError> {
        From(1...10)
        Tap { (_: Int) in reads.withLock { $0 += 1 } }
        FailOnCancellation(SyncError.cancelled)
    }

    let elements = await Task {
        var elements: [Result<Int, SyncError>] = []
        var iterator = pipe.makeAsyncIterator()
        while let element = await iterator.next() {
            elements.append(element)
            if elements.count == 2 { withUnsafeCurrentTask { $0?.cancel() } }
        }
        // Read again after the end.
        #expect(await iterator.next() == nil)
        return elements
    }
    .value

    #expect(elements == [.success(1), .success(2), .failure(.cancelled)])
    #expect(reads.withLock { $0 } == 2, "the source was read after the cancel")
}

@Test
func failOnCancellationWithoutItGivesAPartialSuccess() async {
    let plain = Pipe<Int, SyncError> {
        From([1, 2, 3])
        TapError { (_: SyncError) in }
    }

    #expect(await inCancelledTask { await plain.toResult() } == .success([]))
}

@Test
func failOnCancellationSetsTheFailureTypeOfANeverPipe() async {
    let pipe = Pipe<Int, SyncError> {
        From([1, 2, 3])
        Map { (n: Int) in n * 10 }
        FailOnCancellation(SyncError.cancelled)
    }

    #expect(await pipe.toResult() == .success([10, 20, 30]))
    #expect(await inCancelledTask { await pipe.toResult() } == .failure(.cancelled))
}

@Test
func failOnCancellationFailureGoesThroughLaterStages() async {
    let pipe = Pipe<String, Never> {
        From([1, 2])
        FailOnCancellation(SyncError.cancelled)
        Match(
            onSuccess: { (n: Int) in "value \(n)" },
            onFailure: { (error: SyncError) in "error \(error)" }
        )
    }

    #expect(await pipe.toResult() == .success(["value 1", "value 2"]))
    #expect(await inCancelledTask { await pipe.toResult() } == .success(["error cancelled"]))
}

@Test
func failOnCancellationAfterAConcurrentStage() async {
    let pipe = Pipe<Int, SyncError> {
        From(0..<100)
        AsyncMap(concurrency: 4) { (n: Int) in n }
        FailOnCancellation(SyncError.cancelled)
    }

    let all = await pipe.toResult()
    #expect((try? all.get())?.sorted() == Array(0..<100))
    #expect(await inCancelledTask { await pipe.toResult() } == .failure(.cancelled))
}

@Test
func failOnCancellationInAnOpenPipe() async {
    let open = OpenPipe {
        From(Int.self)
        FailOnCancellation(SyncError.cancelled)
    }

    #expect(await open([1, 2]).toResult() == .success([1, 2]))
    #expect(await inCancelledTask { await open([1, 2]).toResult() } == .failure(.cancelled))
}

@Test
func failOnCancellationCanBeOptional() async {
    @Sendable func make(_ failing: Bool) -> Pipe<Int, SyncError> {
        Pipe<Int, SyncError> {
            From([1, 2])
            TapError { (_: SyncError) in }
            if failing {
                FailOnCancellation(SyncError.cancelled)
            }
        }
    }

    #expect(await inCancelledTask { await make(true).toResult() } == .failure(.cancelled))
    #expect(await inCancelledTask { await make(false).toResult() } == .success([]))
}

@Test
func failOnCancellationPassesEverythingInsideAShield() async {
    guard #available(macOS 27.0, iOS 27.0, tvOS 27.0, watchOS 27.0, visionOS 27.0, *) else {
        return
    }
    let pipe = Pipe<Int, SyncError> {
        From([1, 2, 3])
        FailOnCancellation(SyncError.cancelled)
    }

    let result = await inCancelledTask {
        await withTaskCancellationShield { await pipe.toResult() }
    }
    #expect(result == .success([1, 2, 3]))
}

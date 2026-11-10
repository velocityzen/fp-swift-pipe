@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable, Sendable { case bad }

// MARK: - Outer-task cancellation propagates into in-flight transforms
//
// A closure counts as `completed` only if it was not cancelled during its sleep. If
// cancellation works, `completed` stays near the concurrency limit, far below 1000.

@Test
func cancellingOuterTaskShortCircuitsAsyncMapConcurrent() async {
    let started = Mutex<Int>(0)
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<1_000)
        AsyncMap(concurrency: 4) { (n: Int) async -> Int in
            started.withLock { $0 += 1 }
            try? await Task.sleep(nanoseconds: 20_000_000)
            if Task.isCancelled { return -1 }
            completed.withLock { $0 += 1 }
            return n
        }
    }

    let task = Task {
        var emitted = 0
        for await _ in pipe {
            emitted += 1
            if emitted == 2 { break }
        }
        return emitted
    }
    _ = await task.value

    // 4 running at once, plus one more for each of the 2 elements read.
    let totalStarted = started.withLock { $0 }
    let totalCompleted = completed.withLock { $0 }
    #expect(totalStarted <= 8, "started \(totalStarted): pipeline kept priming after cancel")
    #expect(
        totalCompleted <= 6,
        "completed \(totalCompleted): in-flight tasks didn't bail on cancel"
    )
}

@Test
func cancellingOuterTaskShortCircuitsAsyncMapOrdered() async {
    let started = Mutex<Int>(0)
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<1_000)
        AsyncMap(concurrency: 4, ordered: true) { (n: Int) async -> Int in
            started.withLock { $0 += 1 }
            try? await Task.sleep(nanoseconds: 20_000_000)
            if Task.isCancelled { return -1 }
            completed.withLock { $0 += 1 }
            return n
        }
    }

    let task = Task {
        var emitted = 0
        for await _ in pipe {
            emitted += 1
            if emitted == 2 { break }
        }
        return emitted
    }
    _ = await task.value

    let totalStarted = started.withLock { $0 }
    let totalCompleted = completed.withLock { $0 }
    #expect(
        totalStarted <= 8,
        "started \(totalStarted): ordered pipeline kept priming after cancel"
    )
    #expect(totalCompleted <= 6, "completed \(totalCompleted): ordered in-flight didn't bail")
}

@Test
func cancellingOuterTaskShortCircuitsAsyncFlatMap() async {
    let started = Mutex<Int>(0)
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From(0..<1_000)
        AsyncFlatMap(concurrency: 4) { (n: Int) async -> Result<Int, E> in
            started.withLock { $0 += 1 }
            try? await Task.sleep(nanoseconds: 20_000_000)
            if Task.isCancelled { return .success(-1) }
            completed.withLock { $0 += 1 }
            return .success(n)
        }
    }

    let task = Task {
        var emitted = 0
        for await _ in pipe {
            emitted += 1
            if emitted == 2 { break }
        }
        return emitted
    }
    _ = await task.value

    let totalStarted = started.withLock { $0 }
    let totalCompleted = completed.withLock { $0 }
    #expect(
        totalStarted <= 8,
        "started \(totalStarted): flat-map pipeline kept priming after cancel"
    )
    #expect(totalCompleted <= 6, "completed \(totalCompleted): flat-map in-flight didn't bail")
}

@Test
func cancellingOuterTaskShortCircuitsAsyncMatchError() async {
    let started = Mutex<Int>(0)
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From(0..<1_000)
        FlatMap { (_: Int) -> Result<Int, E> in .failure(.bad) }
        AsyncMatchError(E.bad, concurrency: 4) { () async -> Result<Int, E> in
            started.withLock { $0 += 1 }
            try? await Task.sleep(nanoseconds: 20_000_000)
            if Task.isCancelled { return .success(-1) }
            completed.withLock { $0 += 1 }
            return .success(0)
        }
    }

    let task = Task {
        var emitted = 0
        for await _ in pipe {
            emitted += 1
            if emitted == 2 { break }
        }
        return emitted
    }
    _ = await task.value

    let totalStarted = started.withLock { $0 }
    let totalCompleted = completed.withLock { $0 }
    #expect(totalStarted <= 8, "started \(totalStarted): match pipeline kept priming after cancel")
    #expect(totalCompleted <= 6, "completed \(totalCompleted): match in-flight didn't bail")
}

// MARK: - Time-bounded cancellation
//
// The reader must not wait for closures that still run after it stops.

@Test
func cancellationDoesNotBlockConsumerOnPendingInFlight() async {
    let pipe = Pipe<Int, Never> {
        From(0..<100)
        AsyncMap(concurrency: 4) { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: 200_000_000)
            return n
        }
    }

    let clock = ContinuousClock()
    let elapsed = await clock.measure {
        for await _ in pipe { break }  // stop at the first element
    }
    print("[cancel] consumer unblocked after \(elapsed)")

    // The first element takes about 200ms. Simulators can stall on CI, so the time check
    // runs on macOS and Linux only.
    #if os(macOS) || os(Linux)
    #expect(
        elapsed < .milliseconds(500),
        "consumer waited \(elapsed): cancellation didn't unblock"
    )
    #endif
}

// MARK: - Non-cooperative transforms (pins README's documented limitation)
//
// `Task.cancel()` cannot stop a closure that never checks for cancellation, as the README
// says. If this test fails, the stages now force work to stop: update the README.

@Test
func nonCooperativeTransformsRunToCompletion() async {
    let completed = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From(0..<8)
        AsyncMap(concurrency: 4) { (n: Int) async -> Int in
            // No await and no cancellation check, so it cannot be cancelled.
            var acc = 0
            for i in 0..<200_000 { acc &+= i }
            _ = acc
            completed.withLock { $0 += 1 }
            return n
        }
    }

    let task = Task {
        for await _ in pipe { break }  // stop at the first element
    }
    _ = await task.value

    // Give the started closures time to finish.
    try? await Task.sleep(nanoseconds: 100_000_000)
    let total = completed.withLock { $0 }
    #expect(total >= 4, "expected non-cooperative tasks to finish; got \(total)")
}

// MARK: - Iterator deinit teardown

@Test
func breakingOutOfForAwaitTearsDownPipe() async {
    let totalProduced = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        Defer { (0..<1_000).lazy }
        AsyncMap(concurrency: 1) { (n: Int) async -> Int in
            totalProduced.withLock { $0 += 1 }
            return n
        }
    }

    var seen = 0
    for await _ in pipe {
        seen += 1
        if seen == 5 { break }
    }
    #expect(seen == 5)
    // After `break`, no more elements are made. One extra is OK because of buffering.
    let total = totalProduced.withLock { $0 }
    #expect(total <= 6, "produced \(total): sequential pipe kept producing after break")
}

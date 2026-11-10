@testable import FPPipe
import Synchronization
import Testing

// `bufferLimit` caps how many results may wait for an earlier, slower element when `ordered` is
// true. With `bufferLimit: 0`, no result may wait, so even with `concurrency: 8` the closures
// run one at a time. The stage tests below use that to check that each stage passes the limit on.

private enum E: Error, Equatable, Sendable {
    case failed(Int)
}

/// Eight elements that all reach the closure of the stage under test.
private let ones = Array(repeating: 1, count: 8)

private func failEven(_ n: Int) -> Result<Int, E> {
    n.isMultiple(of: 2) ? .failure(.failed(n)) : .success(n)
}

private func failAll(_ n: Int) -> Result<Int, E> {
    .failure(.failed(n))
}

/// Counts the closures that run at the same time. Each one takes 10 ms.
private final class InFlight: Sendable {
    private let state = Mutex((now: 0, peak: 0))

    var peak: Int {
        state.withLock { $0.peak }
    }

    func run<T: Sendable>(_ value: T) async -> T {
        state.withLock {
            $0.now += 1
            $0.peak = max($0.peak, $0.now)
        }
        try? await Task.sleep(for: .milliseconds(10))
        state.withLock { $0.now -= 1 }
        return value
    }
}

/// Runs the pipe made from `ones`, and checks that all eight elements come out and that the
/// closures ran one at a time.
private func expectOneAtATime<Value, F>(
    sourceLocation: SourceLocation = #_sourceLocation,
    _ makePipe: (InFlight) -> Pipe<Value, F>,
) async {
    let inFlight = InFlight()
    let count = await makePipe(inFlight).toArray().count

    #expect(count == ones.count, sourceLocation: sourceLocation)
    #expect(inFlight.peak == 1, sourceLocation: sourceLocation)
}

// MARK: - The limit

@Test
func bufferLimitCapsResultsWaitingBehindSlowElement() async {
    let started = Mutex(0)
    let (gate, release) = AsyncStream.makeStream(of: Void.self)
    let pipe = Pipe<Int, Never> {
        From(1...100)
        AsyncMap(concurrency: 3, ordered: true, bufferLimit: 4) { (n: Int) async -> Int in
            started.withLock { $0 += 1 }
            if n == 1 {
                for await _ in gate {}
            }
            return n
        }
    }
    let consumer = Task { await pipe.toResult() }

    // Element 1, then the four results that may wait for it.
    for _ in 0..<200 where started.withLock({ $0 }) < 5 {
        try? await Task.sleep(for: .milliseconds(10))
    }
    // Give a stage that ignores the limit time to start more.
    try? await Task.sleep(for: .milliseconds(50))
    let startedWhileBlocked = started.withLock { $0 }
    release.finish()

    #expect(startedWhileBlocked == 5)
    #expect(await consumer.value == .success(Array(1...100)))
}

@Test(arguments: [0, 1, 2])
func bufferLimitBelowConcurrencyLowersClosuresInFlight(bufferLimit: Int) async {
    let inFlight = InFlight()
    let pipe = Pipe<Int, Never> {
        From(1...12)
        AsyncMap(concurrency: 8, ordered: true, bufferLimit: bufferLimit) { (n: Int) async -> Int in
            await inFlight.run(n)
        }
    }

    #expect(await pipe.toResult() == .success(Array(1...12)))
    // The closures after the first unfinished one may finish first, so at most `bufferLimit` of
    // them run beside it.
    #expect(inFlight.peak <= bufferLimit + 1)
}

// MARK: - Every stage passes the limit on

@Test
func successStagesTakeBufferLimit() async {
    await expectOneAtATime { inFlight in
        Pipe<Int, Never> {
            From(ones)
            AsyncMap(concurrency: 8, ordered: true, bufferLimit: 0) { (n: Int) async -> Int in
                await inFlight.run(n)
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            AsyncFlatMap(concurrency: 8, ordered: true, bufferLimit: 0) {
                (n: Int) async -> Result<Int, E> in .success(await inFlight.run(n))
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, Never> {
            From(ones)
            AsyncFilter(concurrency: 8, ordered: true, bufferLimit: 0) { (_: Int) async -> Bool in
                await inFlight.run(true)
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, Never> {
            From(ones)
            AsyncCompactMap(concurrency: 8, ordered: true, bufferLimit: 0) {
                (n: Int) async -> Int? in
                await inFlight.run(n)
            }
        }
    }
}

@Test
func failureStagesTakeBufferLimit() async {
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            FlatMap(failAll)
            AsyncFlatMapError(concurrency: 8, ordered: true, bufferLimit: 0) {
                (_: E) async -> Result<Int, E> in .success(await inFlight.run(1))
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, Never> {
            From(ones)
            FlatMap(failAll)
            AsyncGetOrElse(concurrency: 8, ordered: true, bufferLimit: 0) { (_: E) async -> Int in
                await inFlight.run(1)
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            FlatMap(failAll)
            AsyncAlt(concurrency: 8, ordered: true, bufferLimit: 0) { () async -> Result<Int, E> in
                .success(await inFlight.run(1))
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, Never> {
            From(ones)
            FlatMap(failEven)
            AsyncMatch(
                concurrency: 8,
                ordered: true,
                bufferLimit: 0,
                onSuccess: { (n: Int) async -> Int in await inFlight.run(n) },
                onFailure: { (_: E) async -> Int in 0 }
            )
        }
    }
}

@Test
func matchStagesTakeBufferLimit() async {
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            AsyncMatch(1, concurrency: 8, ordered: true, bufferLimit: 0) {
                () async -> Result<Int, E> in
                .success(await inFlight.run(1))
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            AsyncMatch(Int.self, concurrency: 8, ordered: true, bufferLimit: 0) {
                (n: Int) async -> Result<Int, E> in .success(await inFlight.run(n))
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            AsyncMatch(
                where: { $0 > 0 },
                concurrency: 8,
                ordered: true,
                bufferLimit: 0,
                then: { (n: Int) async -> Result<Int, E> in .success(await inFlight.run(n)) }
            )
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            AsyncMatch(
                extract: { (n: Int) -> Int? in n },
                concurrency: 8,
                ordered: true,
                bufferLimit: 0,
                then: { (n: Int) async -> Result<Int, E> in .success(await inFlight.run(n)) }
            )
        }
    }
}

@Test
func matchErrorStagesTakeBufferLimit() async {
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            FlatMap(failAll)
            AsyncMatchError(E.failed(1), concurrency: 8, ordered: true, bufferLimit: 0) {
                () async -> Result<Int, E> in .success(await inFlight.run(1))
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            FlatMap(failAll)
            AsyncMatchError(E.self, concurrency: 8, ordered: true, bufferLimit: 0) {
                (_: E) async -> Result<Int, E> in .success(await inFlight.run(1))
            }
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            FlatMap(failAll)
            AsyncMatchError(
                where: { (_: E) in true },
                concurrency: 8,
                ordered: true,
                bufferLimit: 0,
                then: { (_: E) async -> Result<Int, E> in .success(await inFlight.run(1)) }
            )
        }
    }
    await expectOneAtATime { inFlight in
        Pipe<Int, E> {
            From(ones)
            FlatMap(failAll)
            AsyncMatchError(
                extract: { (e: E) -> Int? in
                    if case .failed(let n) = e { n } else { nil }
                },
                concurrency: 8,
                ordered: true,
                bufferLimit: 0,
                then: { (n: Int) async -> Result<Int, E> in .success(await inFlight.run(n)) }
            )
        }
    }
}

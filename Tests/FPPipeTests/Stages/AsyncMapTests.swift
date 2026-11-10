@testable import FPPipe
import FP
import Testing

@Test
func asyncMapTransformsSequentially() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        AsyncMap { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: 1_000)
            return n * 100
        }
    }

    let result = await pipe.toResult()
    #expect(result == .success([100, 200, 300]))
}

@Test
func asyncMapConcurrentEmitsUnordered() async {
    // Small numbers sleep less, so they finish first.
    let count = 5
    let pipe = Pipe<Int, Never> {
        From([5, 3, 1, 4, 2])
        AsyncMap(concurrency: count) { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: UInt64(n) * 5_000_000)
            return n
        }
    }

    let result = await pipe.toResult()
    let values = result.getOrElse([])

    #expect(Set(values) == Set([1, 2, 3, 4, 5]))
    // Timing can vary on CI simulators, so check this on macOS and Linux only.
    #if os(macOS) || os(Linux)
    #expect(values.first == 1)
    #endif
}

@Test
func asyncMapConcurrentParallelizesWork() async {
    // Passes only if all `count` closures run at once. It measures no time, so slow CI is fine.
    let count = 5
    let rendezvous = Rendezvous(expected: count)
    let pipe = Pipe<Int, Never> {
        From(0..<count)
        AsyncMap(concurrency: count) { (n: Int) async -> Int in
            await rendezvous.enter()
            return n
        }
    }

    _ = await pipe.toResult()
    #expect(rendezvous.peak == count)
}

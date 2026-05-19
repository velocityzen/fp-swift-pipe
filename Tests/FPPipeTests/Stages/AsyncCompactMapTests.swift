@testable import FPPipe
import FP
import Testing

@Test
func asyncCompactMapDropsNils() async {
    let pipe = Pipe<Int, Never> {
        From(["1", "two", "3", "four", "5"])
        AsyncCompactMap { (s: String) async -> Int? in
            try? await Task.sleep(nanoseconds: 1_000)
            return Int(s)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 3, 5]))
}

@Test
func asyncCompactMapConcurrentRetainsParsedValues() async {
    let pipe = Pipe<Int, Never> {
        From(["1", "two", "3", "four", "5"])
        AsyncCompactMap(concurrency: 5) { Int($0) }
    }
    let result = await pipe.toResult()
    let values = result.getOrElse([])
    #expect(Set(values) == Set([1, 3, 5]))
}

@Test
func asyncCompactMapConcurrentParallelizesWork() async {
    // 10 elements × 20ms each. Sequential bound: 200ms. Concurrent: ~20ms.
    let count = 10
    let perElementMs: UInt64 = 20
    let pipe = Pipe<Int, Never> {
        From(0..<count)
        AsyncCompactMap(concurrency: count) { (n: Int) async -> Int? in
            try? await Task.sleep(nanoseconds: perElementMs * 1_000_000)
            return n
        }
    }

    let clock = ContinuousClock()
    let start = clock.now
    _ = await pipe.toResult()
    let elapsed = start.duration(to: clock.now)
    let observedMs =
        Double(elapsed.components.seconds) * 1_000
        + Double(elapsed.components.attoseconds) / 1e15

    // CI runners (often 2 vCPUs, oversubscribed) can't reliably hit 2×; require ≥10%.
    let sequentialMs = Double(count) * Double(perElementMs)
    #expect(observedMs < sequentialMs * 9 / 10)
}

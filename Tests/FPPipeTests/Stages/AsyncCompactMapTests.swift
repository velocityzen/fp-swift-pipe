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
    // Every transform waits until all `count` are running at once — reachable only if the
    // stage runs them in parallel. No wall-clock bound, so a loaded CI runner can't flake it.
    let count = 5
    let rendezvous = Rendezvous(expected: count)
    let pipe = Pipe<Int, Never> {
        From(0..<count)
        AsyncCompactMap(concurrency: count) { (n: Int) async -> Int? in
            await rendezvous.enter()
            return n
        }
    }

    _ = await pipe.toResult()
    #expect(rendezvous.peak == count)
}

@testable import FPPipe
import Testing

@Test
func asyncMapOrderedPreservesOrderSequential() async {
    let pipe = Pipe<Int, Never> {
        From([5, 3, 1, 4, 2])
        AsyncMap(ordered: true) { (n: Int) async -> Int in n * 10 }
    }
    let result = await pipe.toResult()
    #expect(result == .success([50, 30, 10, 40, 20]))
}

@Test
func asyncMapOrderedPreservesOrderConcurrent() async {
    // Small numbers finish first, but results must stay in source order.
    let pipe = Pipe<Int, Never> {
        From([5, 3, 1, 4, 2])
        AsyncMap(concurrency: 5, ordered: true) { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: UInt64(n) * 1_000_000)
            return n * 10
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([50, 30, 10, 40, 20]))
}

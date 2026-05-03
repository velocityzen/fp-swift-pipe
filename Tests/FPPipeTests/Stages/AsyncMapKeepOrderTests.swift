@testable import FPPipe
import Testing

@Test
func asyncMapKeepOrderPreservesOrderSequential() async {
    // Default concurrency: 1, but order must always be preserved.
    let pipe = Pipe<Int, Never> {
        From([5, 3, 1, 4, 2])
        AsyncMapKeepOrder { (n: Int) async -> Int in n * 10 }
    }
    let result = await pipe.toResult()
    #expect(result == .success([50, 30, 10, 40, 20]))
}

@Test
func asyncMapKeepOrderPreservesOrderConcurrent() async {
    // Smaller numbers finish faster but the output order must match the input.
    let pipe = Pipe<Int, Never> {
        From([5, 3, 1, 4, 2])
        AsyncMapKeepOrder(concurrency: 5) { (n: Int) async -> Int in
            try? await Task.sleep(nanoseconds: UInt64(n) * 1_000_000)
            return n * 10
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([50, 30, 10, 40, 20]))
}

@testable import FPPipe
import Testing

private enum E: Error, Equatable, Sendable { case bad }

@Test
func asyncFlatMapKeepOrderPreservesOrderSequential() async {
    let pipe = Pipe<Int, E> {
        From([5, 3, 1, 4, 2])
        AsyncFlatMapKeepOrder { (n: Int) async -> Result<Int, E> in
            .success(n * 10)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([50, 30, 10, 40, 20]))
}

@Test
func asyncFlatMapKeepOrderPreservesOrderConcurrent() async {
    // Smaller numbers finish faster but the output order must match the input.
    let pipe = Pipe<Int, E> {
        From([5, 3, 1, 4, 2])
        AsyncFlatMapKeepOrder(concurrency: 5) { (n: Int) async -> Result<Int, E> in
            try? await Task.sleep(nanoseconds: UInt64(n) * 1_000_000)
            return .success(n * 10)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([50, 30, 10, 40, 20]))
}

@Test
func asyncFlatMapKeepOrderShortCircuitsOnFailure() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        AsyncFlatMapKeepOrder { (n: Int) async -> Result<Int, E> in
            n == 2 ? .failure(.bad) : .success(n * 10)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .failure(.bad))
}

@Test
func asyncFlatMapKeepOrderEmitsFailuresInSourceOrderConcurrent() async {
    var seen: [Result<Int, E>] = []
    let pipe = Pipe<Int, E> {
        From([1, 2, 3, 4])
        AsyncFlatMapKeepOrder(concurrency: 4) { (n: Int) async -> Result<Int, E> in
            try? await Task.sleep(nanoseconds: UInt64(5 - n) * 1_000_000)
            return n == 2 ? .failure(.bad) : .success(n * 10)
        }
    }
    for await x in pipe { seen.append(x) }
    #expect(seen == [.success(10), .failure(.bad), .success(30), .success(40)])
}

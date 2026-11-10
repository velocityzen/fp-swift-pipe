@testable import FPPipe
import Testing

private enum E: Error, Equatable, Sendable { case bad }

@Test
func asyncFlatMapOrderedPreservesOrderSequential() async {
    let pipe = Pipe<Int, E> {
        From([5, 3, 1, 4, 2])
        AsyncFlatMap(ordered: true) { (n: Int) async -> Result<Int, E> in
            .success(n * 10)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([50, 30, 10, 40, 20]))
}

@Test
func asyncFlatMapOrderedPreservesOrderConcurrent() async {
    // Small numbers finish first, but results must stay in source order.
    let pipe = Pipe<Int, E> {
        From([5, 3, 1, 4, 2])
        AsyncFlatMap(concurrency: 5, ordered: true) { (n: Int) async -> Result<Int, E> in
            try? await Task.sleep(nanoseconds: UInt64(n) * 1_000_000)
            return .success(n * 10)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([50, 30, 10, 40, 20]))
}

@Test
func asyncFlatMapOrderedShortCircuitsOnFailure() async {
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        AsyncFlatMap(ordered: true) { (n: Int) async -> Result<Int, E> in
            n == 2 ? .failure(.bad) : .success(n * 10)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .failure(.bad))
}

@Test
func asyncFlatMapOrderedEmitsFailuresInSourceOrderConcurrent() async {
    var seen: [Result<Int, E>] = []
    let pipe = Pipe<Int, E> {
        From([1, 2, 3, 4])
        AsyncFlatMap(concurrency: 4, ordered: true) { (n: Int) async -> Result<Int, E> in
            try? await Task.sleep(nanoseconds: UInt64(5 - n) * 1_000_000)
            return n == 2 ? .failure(.bad) : .success(n * 10)
        }
    }
    for await x in pipe { seen.append(x) }
    #expect(seen == [.success(10), .failure(.bad), .success(30), .success(40)])
}

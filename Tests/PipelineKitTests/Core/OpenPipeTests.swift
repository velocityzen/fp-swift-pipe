@testable import PipelineKit
import Synchronization
import Testing

private enum E: Error, Equatable, Sendable { case bad }

// MARK: - Basic open-pipe construction

@Test
func openPipeWithSyncSource() async {
    let pipe = OpenPipe {
        From(Int.self)
        Map { (n: Int) in n * 10 }
    }
    let result = await pipe([1, 2, 3]).toResult()
    #expect(result == .success([10, 20, 30]))
}

@Test
func openPipeWithAsyncSource() async {
    let pipe = OpenPipe {
        From(Int.self)
        Map { (n: Int) in n + 1 }
    }
    let stream = AsyncStream<Int> { continuation in
        for n in [10, 20, 30] { continuation.yield(n) }
        continuation.finish()
    }
    let result = await pipe(stream).toResult()
    #expect(result == .success([11, 21, 31]))
}

@Test
func openPipeIsReCallableWithDifferentSources() async {
    let pipe = OpenPipe {
        From(Int.self)
        Filter { $0 > 0 }
        Map { (n: Int) in n * 2 }
    }
    let a = await pipe([1, -1, 2]).toResult()
    let b = await pipe([10, -10, 20, -20]).toResult()
    #expect(a == .success([2, 4]))
    #expect(b == .success([20, 40]))
}

// MARK: - Stage protocol coverage

@Test
func openPipeAcceptsFlatMapAndWidensFailure() async {
    let pipe = OpenPipe {
        From(Int.self)
        FlatMap { (n: Int) -> Result<Int, E> in
            n == 2 ? .failure(.bad) : .success(n * 100)
        }
    }
    let result = await pipe([1, 2, 3]).toResult()
    #expect(result == .failure(.bad))
}

@Test
func openPipeAcceptsForwardingStages() async {
    let pipe = OpenPipe {
        From(Int.self)
        Drop(2)
        Take(2)
        Map { (n: Int) in n }
    }
    let result = await pipe([1, 2, 3, 4, 5, 6]).toResult()
    #expect(result == .success([3, 4]))
}

@Test
func openPipeAcceptsCompactMap() async {
    let pipe = OpenPipe {
        From(String.self)
        CompactMap { (s: String) -> Int? in Int(s) }
    }
    let result = await pipe(["1", "x", "2", "y", "3"]).toResult()
    #expect(result == .success([1, 2, 3]))
}

@Test
func openPipeAcceptsFailureRecovery() async {
    let pipe = OpenPipe {
        From(Int.self)
        FlatMap { (n: Int) -> Result<Int, E> in
            n < 0 ? .failure(.bad) : .success(n)
        }
        FlatMapError { (_: E) -> Result<Int, E> in .success(99) }
    }
    let result = await pipe([1, -1, 2]).toResult()
    #expect(result == .success([1, 99, 2]))
}

@Test
func openPipeAcceptsMatchFold() async {
    let pipe = OpenPipe {
        From(Int.self)
        FlatMap { (n: Int) -> Result<Int, E> in
            n < 0 ? .failure(.bad) : .success(n)
        }
        Match(
            onSuccess: { (n: Int) in "ok:\(n)" },
            onFailure: { (_: E) in "err" },
        )
    }
    let result = await pipe([1, -1, 2]).toResult()
    #expect(result == .success(["ok:1", "err", "ok:2"]))
}

// MARK: - FromAsync<T>() alias

@Test
func fromAsyncOpenSourceIsEquivalentToFrom() async {
    let pipe = OpenPipe {
        FromAsync(Int.self)
        Map { (n: Int) in n * 3 }
    }
    let result = await pipe([1, 2, 3]).toResult()
    #expect(result == .success([3, 6, 9]))
}

// MARK: - Re-iterability of the closed Pipe returned from a call

@Test
func appliedOpenPipeIsReiterable() async {
    let pipe = OpenPipe {
        From(Int.self)
        Map { (n: Int) in n }
    }
    let applied = pipe([1, 2, 3])
    let a = await applied.toResult()
    let b = await applied.toResult()
    #expect(a == b)
    #expect(a == .success([1, 2, 3]))
}

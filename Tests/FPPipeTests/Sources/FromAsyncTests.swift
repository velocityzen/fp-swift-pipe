@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable, Sendable { case bad }

@Test
func fromAsyncWithSyncSequence() async {
    let pipe = Pipe<Int, Never> {
        FromAsync { () async -> [Int] in
            try? await Task.sleep(nanoseconds: 1_000)
            return [1, 2, 3]
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 2, 3]))
}

@Test
func fromAsyncWithAsyncSequence() async {
    let pipe = Pipe<Int, Never> {
        FromAsync { () async -> AsyncStream<Int> in
            try? await Task.sleep(nanoseconds: 1_000)
            return AsyncStream<Int> { continuation in
                continuation.yield(10)
                continuation.yield(20)
                continuation.yield(30)
                continuation.finish()
            }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([10, 20, 30]))
}

@Test
func fromAsyncResultLiftsResultBearingSequence() async {
    let pipe = Pipe<Int, E> {
        FromAsyncResult { () async -> AsyncStream<Result<Int, E>> in
            AsyncStream<Result<Int, E>> { continuation in
                continuation.yield(.success(10))
                continuation.yield(.failure(.bad))
                continuation.yield(.success(20))
                continuation.finish()
            }
        }
    }
    var seen: [Result<Int, E>] = []
    for await x in pipe { seen.append(x) }
    #expect(seen == [.success(10), .failure(.bad), .success(20)])
}

@Test
func fromAsyncIsReiterableAndAwaitsClosureFresh() async {
    let counter = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        FromAsync { () async -> [Int] in
            counter.withLock { $0 += 1 }
            return [1, 2, 3]
        }
    }

    let first = await pipe.toResult()
    let second = await pipe.toResult()

    #expect(first == .success([1, 2, 3]))
    #expect(second == .success([1, 2, 3]))
    // Closure was awaited once per iteration.
    #expect(counter.withLock { $0 } == 2)
}

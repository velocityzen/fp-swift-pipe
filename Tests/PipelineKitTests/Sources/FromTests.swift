@testable import PipelineKit
import Synchronization
import Testing

private enum E: Error, Equatable { case bad }

@Test
func fromLiftsSyncSequenceIntoSuccesses() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        Map { (n: Int) in n * 10 }
    }
    let result = await pipe.toResult()
    #expect(result == .success([10, 20, 30]))
}

@Test
func fromResultLiftsResultBearingSequenceDirectly() async {
    let stream = AsyncStream<Result<Int, E>> { continuation in
        continuation.yield(.success(10))
        continuation.yield(.failure(.bad))
        continuation.yield(.success(20))
        continuation.finish()
    }

    let pipe = Pipe<Int, E> {
        FromResult(stream)
    }

    var seen: [Result<Int, E>] = []
    for await x in pipe {
        seen.append(x)
    }
    #expect(seen == [.success(10), .failure(.bad), .success(20)])
}

@Test
func deferProducesFreshSourcePerIteration() async {
    let counter = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        Defer { () -> [Int] in
            counter.withLock { $0 += 1 }
            return [1, 2, 3]
        }
        Map { (n: Int) in n }
    }

    let first = await pipe.toResult()
    let second = await pipe.toResult()

    #expect(first == .success([1, 2, 3]))
    #expect(second == .success([1, 2, 3]))
    #expect(counter.withLock { $0 } == 2)
}

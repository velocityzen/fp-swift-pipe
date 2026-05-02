@testable import PipelineKit
import Testing

private enum AppError: Error, Equatable { case bad }

@Test
func flatMapAsyncSequenceFansOutEachSuccess() async {
    let pipe = Pipe<Int, Never> {
        From([2, 3])
        FlatMapAsyncSequence { (n: Int) in
            AsyncStream<Int> { continuation in
                for i in 0..<n {
                    continuation.yield(i)
                }
                continuation.finish()
            }
        }
    }

    let result = await pipe.toResult()
    #expect(result == .success([0, 1, 0, 1, 2]))
}

@Test
func flatMapAsyncSequencePassesFailuresThrough() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 2 ? .failure(.bad) : .success(n)
        }
        FlatMapAsyncSequence { (n: Int) in
            AsyncStream<Int> { continuation in
                continuation.yield(n * 10)
                continuation.yield(n * 100)
                continuation.finish()
            }
        }
    }

    var observed: [Result<Int, AppError>] = []
    for await element in pipe {
        observed.append(element)
    }

    #expect(
        observed == [
            .success(10), .success(100),
            .failure(.bad),
            .success(30), .success(300),
        ]
    )
}

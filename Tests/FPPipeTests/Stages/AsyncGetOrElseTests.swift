@testable import FPPipe
import Testing

private enum AppError: Error, Equatable, Sendable { case bad }

@Test
func asyncGetOrElseReplacesFailuresAsynchronously() async {
    let pipe = Pipe<Int, Never> {
        From([10, 20, 30])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 20 ? .failure(.bad) : .success(n)
        }
        AsyncGetOrElse { (_: AppError) async -> Int in
            try? await Task.sleep(nanoseconds: 1_000)
            return -1
        }
    }

    let result = await pipe.toResult()
    #expect(result == .success([10, -1, 30]))
}

@Test
func asyncGetOrElseConcurrentReplacesUnordered() async {
    // concurrency > 1 takes the unordered path: elements emit as they complete,
    // so compare contents rather than order.
    let pipe = Pipe<Int, Never> {
        From([10, 20, 30, 40])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n.isMultiple(of: 20) ? .failure(.bad) : .success(n)
        }
        AsyncGetOrElse(concurrency: 4) { (_: AppError) async -> Int in
            try? await Task.sleep(nanoseconds: 1_000)
            return -1
        }
    }

    let result = await pipe.toResult()
    #expect(result.map { $0.sorted() } == .success([-1, -1, 10, 30]))
}

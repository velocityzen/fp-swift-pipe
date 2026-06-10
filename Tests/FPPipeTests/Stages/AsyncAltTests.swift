@testable import FPPipe
import Testing

private enum AppError: Error, Equatable, Sendable { case bad }

@Test
func asyncAltRecoversAsynchronously() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 2 ? .failure(.bad) : .success(n)
        }
        AsyncAlt {
            try? await Task.sleep(nanoseconds: 1_000)
            return Result<Int, AppError>.success(99)
        }
    }

    let result = await pipe.toResult()
    #expect(result == .success([1, 99, 3]))
}

@Test
func asyncAltConcurrentRecoversUnordered() async {
    // concurrency > 1 takes the unordered path: elements emit as they complete,
    // so compare contents rather than order.
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3, 4])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n.isMultiple(of: 2) ? .failure(.bad) : .success(n)
        }
        AsyncAlt(concurrency: 4) {
            try? await Task.sleep(nanoseconds: 1_000)
            return Result<Int, AppError>.success(99)
        }
    }

    let result = await pipe.toResult()
    #expect(result.map { $0.sorted() } == .success([1, 3, 99, 99]))
}

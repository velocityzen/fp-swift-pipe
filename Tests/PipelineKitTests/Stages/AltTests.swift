@testable import PipelineKit
import Testing

private enum AppError: Error, Equatable, Sendable { case bad }

@Test
func altReplacesFailureWithoutSeeingError() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 2 ? .failure(.bad) : .success(n)
        }
        Alt { Result<Int, AppError>.success(99) }
    }

    let result = await pipe.toResult()
    #expect(result == .success([1, 99, 3]))
}

@Test
func altCanReFail() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 2 ? .failure(.bad) : .success(n)
        }
        Alt { Result<Int, AppError>.failure(.bad) }
    }

    let result = await pipe.toResult()
    #expect(result == .failure(.bad))
}

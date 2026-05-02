@testable import PipelineKit
import Testing

private enum AppError: Error, Equatable, Sendable { case bad }

@Test
func getOrElseReplacesFailuresWithComputedFallback() async {
    let pipe = Pipe<Int, Never> {
        From([10, 20, 30])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 20 ? .failure(.bad) : .success(n)
        }
        GetOrElse { (_: AppError) in -1 }
    }

    let result = await pipe.toResult()
    #expect(result == .success([10, -1, 30]))
}

@Test
func getOrElsePassesSuccessesThrough() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in .success(n) }
        GetOrElse { (_: AppError) in -1 }
    }

    let result = await pipe.toResult()
    #expect(result == .success([1, 2, 3]))
}

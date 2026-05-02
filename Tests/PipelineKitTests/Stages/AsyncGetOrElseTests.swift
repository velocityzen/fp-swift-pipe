@testable import PipelineKit
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

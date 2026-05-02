@testable import PipelineKit
import Testing

private enum NetError: Error, Equatable, Sendable { case timeout }
private enum AppError: Error, Equatable, Sendable { case wrapped, fatal }

@Test
func flatMapErrorRecoversFailureIntoSuccess() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, NetError> in
            n == 2 ? .failure(.timeout) : .success(n)
        }
        FlatMapError { (_: NetError) -> Result<Int, AppError> in .success(99) }
    }

    let result = await pipe.toResult()
    #expect(result == .success([1, 99, 3]))
}

@Test
func flatMapErrorReFailsWithDifferentErrorType() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, NetError> in
            n == 2 ? .failure(.timeout) : .success(n)
        }
        FlatMapError { (_: NetError) -> Result<Int, AppError> in .failure(.wrapped) }
    }

    let result = await pipe.toResult()
    #expect(result == .failure(.wrapped))
}

@Test
func flatMapErrorOnSuccessPathPassesThrough() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, NetError> in .success(n + 100) }
        FlatMapError { (_: NetError) -> Result<Int, AppError> in .failure(.fatal) }
    }

    let result = await pipe.toResult()
    #expect(result == .success([101, 102, 103]))
}

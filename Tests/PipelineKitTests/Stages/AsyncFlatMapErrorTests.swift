@testable import PipelineKit
import Testing

private enum NetError: Error, Equatable, Sendable { case timeout }
private enum AppError: Error, Equatable, Sendable { case wrapped }

@Test
func asyncFlatMapErrorRecoversAsynchronously() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, NetError> in
            n == 2 ? .failure(.timeout) : .success(n)
        }
        AsyncFlatMapError { (_: NetError) async -> Result<Int, AppError> in
            try? await Task.sleep(nanoseconds: 1_000)
            return .success(99)
        }
    }

    let result = await pipe.toResult()
    #expect(result == .success([1, 99, 3]))
}

@Test
func asyncFlatMapErrorCanReFail() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, NetError> in
            n == 2 ? .failure(.timeout) : .success(n)
        }
        AsyncFlatMapError { (_: NetError) async -> Result<Int, AppError> in .failure(.wrapped) }
    }

    let result = await pipe.toResult()
    #expect(result == .failure(.wrapped))
}

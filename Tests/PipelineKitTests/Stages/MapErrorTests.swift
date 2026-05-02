@testable import PipelineKit
import Testing

private enum AppError: Error, Equatable {
    case parse(String)
    case empty
}

private enum NetError: Error, Equatable {
    case timeout
}

@Test
func mapErrorWidensFailureType() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, NetError> in
            n == 2 ? .failure(.timeout) : .success(n)
        }
        MapError { (e: NetError) in AppError.parse("net:\(e)") }
    }

    let result = await pipe.toResult()
    #expect(result == .failure(.parse("net:timeout")))
}

@Test
func mapErrorOnSuccessPathPassesThrough() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, NetError> in .success(n + 100) }
        MapError { (_: NetError) in AppError.empty }
    }

    let result = await pipe.toResult()
    #expect(result == .success([101, 102, 103]))
}

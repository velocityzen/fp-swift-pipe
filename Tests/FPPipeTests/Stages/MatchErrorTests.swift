@testable import FPPipe
import Synchronization
import Testing

private enum AppError: Error, Equatable, Sendable {
    case rateLimited
    case fatal
    case http(Int)

    var isRetryable: Bool { self == .rateLimited }
    var statusCode: Int? {
        if case .http(let code) = self { code } else { nil }
    }
}

private struct NetError: Error, Equatable { let code: Int }
private struct DiskError: Error {}

@Test
func matchErrorValueRecoversOnlyEqualFailures() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            switch n {
                case 2: .failure(.rateLimited)
                case 3: .failure(.fatal)
                default: .success(n)
            }
        }
        MatchError(AppError.rateLimited) { .success(0) }
    }

    let elements = await pipe.toArray()
    #expect(elements == [.success(1), .success(0), .failure(.fatal)])
}

@Test
func matchErrorValueCanReFailWithAnotherError() async {
    let pipe = Pipe<Int, AppError> {
        From([1])
        FlatMap { (_: Int) -> Result<Int, AppError> in .failure(.rateLimited) }
        MatchError(AppError.rateLimited) { Result<Int, AppError>.failure(.fatal) }
    }

    #expect(await pipe.toArray() == [.failure(.fatal)])
}

@Test
func matchErrorTypeMatchesInsideAnyErrorChannel() async throws {
    let pipe = Pipe<Int, any Error> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, any Error> in
            switch n {
                case 2: .failure(NetError(code: 503))
                case 3: .failure(DiskError())
                default: .success(n)
            }
        }
        MatchError(NetError.self) { e -> Result<Int, any Error> in .success(e.code) }
    }

    let elements = await pipe.toArray()
    #expect(elements.count == 3)
    #expect(try elements[0].get() == 1)
    #expect(try elements[1].get() == 503)
    #expect(throws: DiskError.self) { try elements[2].get() }
}

@Test
func matchErrorPredicateHandlesAcceptedFailures() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 1 ? .failure(.rateLimited) : .failure(.fatal)
        }
        MatchError(where: \AppError.isRetryable) { _ -> Result<Int, AppError> in .success(-1) }
    }

    #expect(await pipe.toArray() == [.success(-1), .failure(.fatal)])
}

@Test
func matchErrorExtractHandsTheHandlerThePayload() async {
    let pipe = Pipe<Int, AppError> {
        From([404, 0])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 0 ? .failure(.fatal) : .failure(.http(n))
        }
        MatchError(extract: \AppError.statusCode) { code in .success(code + 1) }
    }

    #expect(await pipe.toArray() == [.success(405), .failure(.fatal)])
}

@Test
func matchErrorLeavesSuccessesAloneAndNeverTestsThem() async {
    let tested = Mutex(0)
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in n == 2 ? .failure(.fatal) : .success(n) }
        MatchError(
            where: { (_: AppError) in
                tested.withLock { $0 += 1 }
                return true
            },
            then: { _ -> Result<Int, AppError> in .success(0) }
        )
    }

    #expect(await pipe.toResult() == .success([1, 0, 3]))
    #expect(tested.withLock { $0 } == 1)
}

@Test
func matchErrorComposesInIfWithoutElse() async {
    func make(_ lenient: Bool) -> Pipe<Int, AppError> {
        Pipe<Int, AppError> {
            From([1, 2])
            FlatMap { (n: Int) -> Result<Int, AppError> in
                n == 2 ? .failure(.rateLimited) : .success(n)
            }
            if lenient {
                MatchError(AppError.rateLimited) { .success(0) }
            }
        }
    }

    #expect(await make(true).toArray() == [.success(1), .success(0)])
    #expect(await make(false).toArray() == [.success(1), .failure(.rateLimited)])
}

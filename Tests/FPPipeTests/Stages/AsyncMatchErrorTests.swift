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

private struct NetError: Error { let code: Int }
private struct DiskError: Error {}

@Test
func asyncMatchErrorValueRecoversOnlyEqualFailures() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            switch n {
                case 2: .failure(.rateLimited)
                case 3: .failure(.fatal)
                default: .success(n)
            }
        }
        AsyncMatchError(AppError.rateLimited) { () async -> Result<Int, AppError> in
            try? await Task.sleep(nanoseconds: 1_000)
            return .success(0)
        }
    }

    #expect(await pipe.toArray() == [.success(1), .success(0), .failure(.fatal)])
}

@Test
func asyncMatchErrorTypeMatchesInsideAnyErrorChannel() async throws {
    let pipe = Pipe<Int, any Error> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, any Error> in
            switch n {
                case 2: .failure(NetError(code: 503))
                case 3: .failure(DiskError())
                default: .success(n)
            }
        }
        AsyncMatchError(NetError.self) { e async -> Result<Int, any Error> in .success(e.code) }
    }

    let elements = await pipe.toArray()
    #expect(try elements[0].get() == 1)
    #expect(try elements[1].get() == 503)
    #expect(throws: DiskError.self) { try elements[2].get() }
}

@Test
func asyncMatchErrorPredicateAndExtractHandleTheirMatches() async {
    let pipe = Pipe<Int, AppError> {
        From([0, 1, 2])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            switch n {
                case 0: .failure(.rateLimited)
                case 1: .failure(.http(404))
                default: .failure(.fatal)
            }
        }
        AsyncMatchError(where: \AppError.isRetryable) { _ async -> Result<Int, AppError> in
            .success(-1)
        }
        AsyncMatchError(extract: \AppError.statusCode) { code async in .success(code) }
    }

    #expect(await pipe.toArray() == [.success(-1), .success(404), .failure(.fatal)])
}

@Test
func asyncMatchErrorConcurrentRunsHandlersInParallel() async {
    let count = 4
    let rendezvous = Rendezvous(expected: count)
    let pipe = Pipe<Int, AppError> {
        From(0..<count)
        FlatMap { (_: Int) -> Result<Int, AppError> in .failure(.rateLimited) }
        AsyncMatchError(AppError.rateLimited, concurrency: count) {
            () async -> Result<Int, AppError> in
            await rendezvous.enter()
            return .success(0)
        }
    }

    #expect(await pipe.toResult() == .success([0, 0, 0, 0]))
    #expect(rendezvous.peak == count)
}

@Test
func asyncMatchErrorConcurrentTestsEachFailureOnce() async {
    let tested = Mutex(0)
    let pipe = Pipe<Int, AppError> {
        From(1...6)
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n.isMultiple(of: 2) ? .failure(.http(n)) : .success(n)
        }
        AsyncMatchError(
            extract: { (e: AppError) in
                tested.withLock { $0 += 1 }
                return e.statusCode
            },
            concurrency: 3,
            then: { code async in
                try? await Task.sleep(nanoseconds: 1_000)
                return .success(code * 100)
            }
        )
    }

    let result = await pipe.toResult().map { $0.sorted() }
    #expect(result == .success([1, 3, 5, 200, 400, 600]))
    #expect(tested.withLock { $0 } == 3)
}

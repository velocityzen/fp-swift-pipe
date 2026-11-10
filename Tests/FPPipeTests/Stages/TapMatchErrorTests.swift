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

private let failures: [AppError] = [.rateLimited, .http(404), .fatal, .http(500), .rateLimited]

/// A pipe that gives `1`, then every error in `failures`.
private func failing() -> Pipe<Int, AppError> {
    Pipe<Int, AppError> {
        From([nil] + failures.map(Optional.some))
        FlatMap { (error: AppError?) -> Result<Int, AppError> in
            error.map { .failure($0) } ?? .success(1)
        }
    }
}

@Test
func tapMatchErrorValueRunsOnlyForEqualFailures() async {
    let count = Mutex(0)
    let pipe = Pipe<Int, AppError> {
        FromResult(failing())
        TapMatchError(AppError.rateLimited) { count.withLock { $0 += 1 } }
    }

    #expect(await pipe.toArray() == [.success(1)] + failures.map { .failure($0) })
    #expect(count.withLock { $0 } == 2)
}

@Test
func tapMatchErrorTypeMatchesInsideAnyErrorChannel() async {
    let codes = Mutex<[Int]>([])
    let pipe = Pipe<Int, any Error> {
        From([1, 2, 3, 4])
        FlatMap { (n: Int) -> Result<Int, any Error> in
            switch n {
                case 2: .failure(NetError(code: 503))
                case 3: .failure(DiskError())
                case 4: .failure(NetError(code: 504))
                default: .success(n)
            }
        }
        TapMatchError(NetError.self) { e in codes.withLock { $0.append(e.code) } }
    }

    #expect(await pipe.toArray().count == 4)
    #expect(codes.withLock { $0 } == [503, 504])
}

@Test
func tapMatchErrorPredicateRunsForAcceptedFailures() async {
    let seen = Mutex<[AppError]>([])
    let pipe = Pipe<Int, AppError> {
        FromResult(failing())
        TapMatchError(where: \AppError.isRetryable) { e in seen.withLock { $0.append(e) } }
    }

    _ = await pipe.toArray()
    #expect(seen.withLock { $0 } == [.rateLimited, .rateLimited])
}

@Test
func tapMatchErrorExtractRunsWithThePayload() async {
    let codes = Mutex<[Int]>([])
    let pipe = Pipe<Int, AppError> {
        FromResult(failing())
        TapMatchError(extract: \AppError.statusCode) { code in codes.withLock { $0.append(code) } }
    }

    _ = await pipe.toArray()
    #expect(codes.withLock { $0 } == [404, 500])
}

@Test
func tapMatchErrorIgnoresSuccesses() async {
    let count = Mutex(0)
    let pipe = Pipe<Int, AppError> {
        From([1, 2])
        FlatMap { (n: Int) -> Result<Int, AppError> in .success(n) }
        TapMatchError(where: { (_: AppError) in true }, then: { _ in count.withLock { $0 += 1 } })
        TapMatchError(AppError.self) { _ in count.withLock { $0 += 1 } }
    }

    #expect(await pipe.toResult() == .success([1, 2]))
    #expect(count.withLock { $0 } == 0)
}

@Test
func tapMatchErrorComposesInIfWithoutElse() async {
    let count = Mutex(0)
    func make(_ observe: Bool) -> Pipe<Int, AppError> {
        Pipe<Int, AppError> {
            FromResult(failing())
            if observe {
                TapMatchError(AppError.fatal) { count.withLock { $0 += 1 } }
            }
            if observe {
                TapMatchError(NetError.self) { _ in }
            }
        }
    }

    _ = await make(false).toArray()
    #expect(count.withLock { $0 } == 0)
    _ = await make(true).toArray()
    #expect(count.withLock { $0 } == 1)
}

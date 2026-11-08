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
func asyncTapMatchErrorRunsEachFormForItsMatchesInOrder() async {
    let log = Mutex<[String]>([])
    let failures: [AppError] = [.rateLimited, .http(404), .fatal, .http(500)]
    let pipe = Pipe<Int, AppError> {
        From(failures)
        FlatMap { (e: AppError) -> Result<Int, AppError> in .failure(e) }
        AsyncTapMatchError(AppError.fatal) {
            try? await Task.sleep(nanoseconds: 1_000)
            log.withLock { $0.append("fatal") }
        }
        AsyncTapMatchError(extract: \AppError.statusCode) { code async in
            log.withLock { $0.append("http:\(code)") }
        }
        AsyncTapMatchError(where: \AppError.isRetryable) { _ async in
            log.withLock { $0.append("retry") }
        }
    }

    #expect(await pipe.toArray() == failures.map { .failure($0) })
    #expect(log.withLock { $0 } == ["retry", "http:404", "fatal", "http:500"])
}

@Test
func asyncTapMatchErrorTypeMatchesInsideAnyErrorChannel() async {
    let codes = Mutex<[Int]>([])
    let pipe = Pipe<Int, any Error> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, any Error> in
            switch n {
                case 2: .failure(NetError(code: 503))
                case 3: .failure(DiskError())
                default: .success(n)
            }
        }
        AsyncTapMatchError(NetError.self) { e async in codes.withLock { $0.append(e.code) } }
    }

    #expect(await pipe.toArray().count == 3)
    #expect(codes.withLock { $0 } == [503])
}

@Test
func asyncTapMatchErrorIgnoresSuccesses() async {
    let count = Mutex(0)
    let pipe = Pipe<Int, AppError> {
        From([1, 2])
        FlatMap { (n: Int) -> Result<Int, AppError> in .success(n) }
        AsyncTapMatchError(
            where: { (_: AppError) in true },
            then: { _ async in count.withLock { $0 += 1 } }
        )
    }

    #expect(await pipe.toResult() == .success([1, 2]))
    #expect(count.withLock { $0 } == 0)
}

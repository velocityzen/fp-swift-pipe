// Examples from "Pattern matching" in Readme.md and the DocC articles. Keep all three
// copies the same.

import FPPipe
import Foundation
import Synchronization
import Testing
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

private enum Event: Equatable, Sendable {
    case login(String)
    case logout

    // The user name, for a login only.
    var loggedInUser: String? {
        if case .login(let user) = self { user } else { nil }
    }
}

private enum AppError: Error, Equatable, Sendable {
    case rateLimited
    case http(Int)

    var isRetryable: Bool { self == .rateLimited }

    // The code, for an HTTP error only.
    var statusCode: Int? {
        if case .http(let code) = self { code } else { nil }
    }
}

private let events: [Event] = [.login("ann"), .logout]

private func stream<V: Sendable>(
    _ elements: [Result<V, AppError>]
) -> AsyncStream<Result<V, AppError>> {
    AsyncStream { continuation in
        for element in elements { continuation.yield(element) }
        continuation.finish()
    }
}

private func download(_ url: URL) async -> Result<String, AppError> {
    switch url.lastPathComponent {
        case "empty": .success("")
        case "busy": .failure(.rateLimited)
        case "missing": .failure(.http(404))
        default: .success("page \(url.lastPathComponent)")
    }
}

@Test
func readmePatternValueForm() async {
    let log = Mutex<[String]>([])
    let logouts = Pipe<Event, Never> {
        From(events)
        TapMatch(Event.logout) { log.withLock { $0.append("logged out") } }
    }
    #expect(await logouts.toResult() == .success(events))
    #expect(log.withLock { $0 } == ["logged out"])

    let recovered = Pipe<String, AppError> {
        FromResult(stream([Result<String, AppError>.failure(.rateLimited), .failure(.http(500))]))
        MatchError(AppError.rateLimited) { .success("busy, try later") }
    }
    #expect(await recovered.toArray() == [.success("busy, try later"), .failure(.http(500))])
}

@Test
func readmePatternTypeForm() async {
    let codes = Mutex<[URLError.Code]>([])
    let pipe = Pipe<Int, any Error> {
        From([1, 2])
        FlatMap { (n: Int) -> Result<Int, any Error> in
            n == 1 ? .failure(URLError(.timedOut)) : .success(n)
        }
        TapMatchError(URLError.self) { error in codes.withLock { $0.append(error.code) } }
    }

    #expect(await pipe.toArray().count == 2)
    #expect(codes.withLock { $0 } == [.timedOut])
}

@Test
func readmePatternTypeFormNeedsAReturnType() async {
    let pipe = Pipe<Data, any Error> {
        From([1])
        FlatMap { (_: Int) -> Result<Data, any Error> in .failure(URLError(.badURL)) }
        MatchError(URLError.self) { _ -> Result<Data, any Error> in .success(Data()) }
    }

    #expect(await pipe.firstSuccess() == Data())
}

@Test
func readmePatternConditionForm() async {
    let log = Mutex<[String]>([])
    let pipe = Pipe<Int, AppError> {
        FromResult(stream([.failure(.rateLimited), .failure(.http(500)), .success(2_000)]))
        TapMatchError(where: \AppError.isRetryable) { error in
            log.withLock { $0.append("will retry: \(error)") }
        }
        TapMatch(
            where: { (n: Int) in n > 1_000 },
            then: { n in log.withLock { $0.append("large: \(n)") } }
        )
    }

    #expect(await pipe.toArray().count == 3)
    #expect(log.withLock { $0 } == ["will retry: rateLimited", "large: 2000"])
}

@Test
func readmePatternPayloadForm() async {
    let log = Mutex<[String]>([])
    let users = Pipe<Event, Never> {
        From(events)
        TapMatch(extract: \Event.loggedInUser) { user in
            log.withLock { $0.append("welcome, \(user)") }
        }
    }
    let failures = Pipe<Int, AppError> {
        FromResult(stream([.failure(.rateLimited), .failure(.http(500)), .success(1)]))
        TapMatchError(extract: \AppError.statusCode) { code in
            log.withLock { $0.append("HTTP error \(code)") }
        }
    }

    _ = await users.toResult()
    _ = await failures.toArray()
    #expect(log.withLock { $0 } == ["welcome, ann", "HTTP error 500"])
}

@Test
func readmePatternStagesInARow() async {
    let urls = ["a", "empty", "busy", "missing"].compactMap {
        URL(string: "https://example.com/\($0)")
    }

    let pages = Pipe<String, AppError> {
        From(urls)
        AsyncFlatMap { (url: URL) in await download(url) }
        Match("") { .failure(AppError.http(204)) }  // an empty page becomes a failure
        MatchError(AppError.rateLimited) { .success("busy, try later") }
        TapMatchError(extract: \AppError.statusCode) { code in print(code) }
    }

    #expect(
        await pages.toArray() == [
            .success("page a"),
            .failure(.http(204)),
            .success("busy, try later"),
            .failure(.http(404)),
        ]
    )
}

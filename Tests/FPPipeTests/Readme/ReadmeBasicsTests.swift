// Examples from "Quick start" to "Open pipes" in Readme.md and the DocC articles. Keep
// all three copies the same. The rest of "Calling throwing code" is in
// ReadmeURLSessionTests, which runs on Apple platforms only.

import FP
import FPPipe
import Synchronization
import Testing

private enum ParseError: Error, Equatable {
    case notANumber(String)
}

private enum FetchError: Error, Equatable {
    case timeout
    case notFound
}

private struct Database: Sendable {
    func userIDs() async -> [Int] { [1, 2] }
}

private func makeStream() -> AsyncStream<Int> {
    AsyncStream { continuation in
        continuation.yield(1)
        continuation.yield(2)
        continuation.finish()
    }
}

// MARK: Polling

private struct Status: Sendable, Equatable {
    let isReady: Bool
}

private struct CheckError: Error, Equatable {}

/// Counts its calls. Status checks fail on the first call and are not ready on the
/// second.
private final class Server: Sendable {
    let calls = Mutex(0)

    func status() async -> Status {
        Status(isReady: next() > 2)
    }

    func check() async -> Result<Status, CheckError> {
        next() == 1 ? .failure(CheckError()) : .success(Status(isReady: calls.withLock { $0 } > 2))
    }

    private func next() -> Int {
        calls.withLock { calls in
            calls += 1
            return calls
        }
    }
}

// MARK: Calling throwing code

private enum LoadError: Error {
    case network(any Error)
}

private struct SocketClosed: Error {}

private struct Socket: Sendable {
    /// Gives two messages, then fails.
    func messages() -> AsyncThrowingStream<String, any Error> {
        AsyncThrowingStream { continuation in
            continuation.yield("hello")
            continuation.yield("world")
            continuation.finish(throwing: SocketClosed())
        }
    }
}

/// The `numbers` pipe from "How it works".
private func parsedNumbers() -> Pipe<Int, ParseError> {
    Pipe<Int, ParseError> {
        From(["1", "x", "3"])
        FlatMap { (text: String) -> Result<Int, ParseError> in
            guard let n = Int(text) else {
                return .failure(.notANumber(text))
            }
            return .success(n)
        }
        Map { (n: Int) in n * 2 }  // runs for 1 and 3 only
    }
}

@Test
func readmeQuickStart() async {
    let pipe = Pipe<Int, Never> {
        From(1...6)  // 1, 2, 3, 4, 5, 6
        Filter { (n: Int) in n.isMultiple(of: 2) }  // 2, 4, 6
        Map { (n: Int) in n * 10 }  // 20, 40, 60
    }

    let result = await pipe.toResult()
    #expect(result == .success([20, 40, 60]))
    // Reading it again runs it again.
    #expect(await pipe.toResult() == .success([20, 40, 60]))
}

@Test
func readmeHowItWorks() async {
    let numbers = parsedNumbers()

    #expect(await numbers.toArray() == [.success(2), .failure(.notANumber("x")), .success(6)])
    #expect(await numbers.toResult() == .failure(.notANumber("x")))
}

@Test
func readmeSources() async {
    let stream = makeStream()
    let numbers = parsedNumbers()
    let database = Database()

    #expect(await Pipe<Int, Never> { From([1, 2, 3]) }.toResult() == .success([1, 2, 3]))
    #expect(await Pipe<Int, Never> { From(1...3) }.toResult() == .success([1, 2, 3]))
    #expect(await Pipe<Int, Never> { From(stream) }.toResult() == .success([1, 2]))
    #expect(await Pipe<Int, ParseError> { FromResult(numbers) }.toArray().count == 3)
    #expect(await Pipe<Int, Never> { Success(42) }.toResult() == .success([42]))
    #expect(await Pipe<Int, Never> { Of(42) }.toResult() == .success([42]))
    #expect(
        await Pipe<Int, ParseError> { Failure(ParseError.notANumber("y"), valueType: Int.self) }
            .toResult() == .failure(.notANumber("y"))
    )
    #expect(
        await Pipe<Int, ParseError> { Empty(valueType: Int.self, failureType: ParseError.self) }
            .toResult() == .success([])
    )
    #expect(
        await Pipe<Int, Never> { FromAsync { await database.userIDs() } }.toResult()
            == .success([1, 2])
    )
}

@Test
func readmeRunningAPipeMoreThanOnce() async {
    let pipe = Pipe<Int, Never> {
        Defer { makeStream() }  // a new stream on every run
        Map { (n: Int) in n + 1 }
    }
    #expect(await pipe.toResult() == .success([2, 3]))
    #expect(await pipe.toResult() == .success([2, 3]))

    // A stored stream is empty the second time, with no error.
    let stored = makeStream()
    let once = Pipe<Int, Never> { From(stored) }
    #expect(await once.toResult() == .success([1, 2]))
    #expect(await once.toResult() == .success([]))
}

@Test
func readmePolling() async {
    let server = Server()
    let statuses = Pipe<Status, Never> {
        From(withRepeat(interval: .milliseconds(1)) { await server.status() })
        Take(3)
    }

    #expect(
        await statuses.toResult()
            == .success([Status(isReady: false), Status(isReady: false), Status(isReady: true)])
    )
    // Each run makes its own calls.
    #expect(await statuses.toArray().count == 3)
    #expect(server.calls.withLock { $0 } == 6)
}

@Test
func readmePollingResults() async {
    let server = Server()
    let check: @Sendable () async -> Result<Status, CheckError> = { await server.check() }

    let ready = await Pipe<Status, CheckError> {
        FromResult(withRepeat(interval: .milliseconds(1)) { await check() })
        Filter { (status: Status) in status.isReady }
    }
    .firstSuccess()

    #expect(ready == Status(isReady: true))
    // A failed check, one that was not ready, and a ready one.
    #expect(server.calls.withLock { $0 } == 3)
}

@Test
func readmePollingUntilSuccess() async {
    let server = Server()
    let check: @Sendable () async -> Result<Status, CheckError> = { await server.check() }

    let checks = Pipe<Status, CheckError> {
        FromResult(withRepeat(interval: .milliseconds(1)) { await check() })
        Filter { (status: Status) in status.isReady }
        UntilSuccess()
    }

    var printed: [Result<Status, CheckError>] = []
    for await check in checks {
        printed.append(check)
    }

    // The failed check, then the ready status. The one that was not ready is filtered out.
    #expect(printed == [.failure(CheckError()), .success(Status(isReady: true))])
    #expect(server.calls.withLock { $0 } == 3)
}

@Test
func readmeReadingResults() async {
    let counts = Pipe<Int, Never> { From(1...4) }

    #expect(await counts.reduce(0) { sum, n in sum + n } == .success(10))
    #expect(await counts.firstSuccess() == 1)
    #expect(await counts.lastSuccess() == 4)

    let parity = await counts.reduce(into: [Bool: [Int]]()) { by, n in
        by[n.isMultiple(of: 2), default: []].append(n)
    }
    #expect(parity == .success([false: [1, 3], true: [2, 4]]))

    let numbers = parsedNumbers()
    let separated = await numbers.separate()
    #expect(separated.successes == [2, 6])
    #expect(separated.failures == [.notANumber("x")])
    #expect(await numbers.firstSuccessOrLastError() == .success(2))

    var printed: [String] = []
    for await element in numbers {
        switch element {
            case .success(let n): printed.append("value: \(n)")
            case .failure(let error): printed.append("error: \(error)")
        }
    }
    #expect(printed == ["value: 2", #"error: notANumber("x")"#, "value: 6"])
}

@Test
func readmeReadingResultsWithGet() async {
    let numbers = parsedNumbers()

    var printed: [String] = []
    do throws(ParseError) {
        for try await n in numbers.get() {
            printed.append("value: \(n)")
        }
    } catch {
        printed.append("error: \(error)")
    }
    #expect(printed == ["value: 2", #"error: notANumber("x")"#])

    // The operators before a sink, as the Readme names them.
    #expect(await numbers.untilSuccess().last() == .success(2))
    #expect(await numbers.untilFailure().last() == .failure(.notANumber("x")))
}

@Test
func readmeToResultCanRunAPipeForItsStages() async {
    let seen = Mutex<[Int]>([])
    let pipe = Pipe<Int, Never> {
        From(1...3)
        Tap { (n: Int) in seen.withLock { $0.append(n) } }
    }

    // No `_ =`: `toResult()` is discardable.
    await pipe.toResult()
    #expect(seen.withLock { $0 } == [1, 2, 3])
}

@Test
func readmeThrowingAsyncSequence() async throws {
    let socket = Socket()

    let messages = Pipe<String, LoadError> {
        DeferResult { socket.messages().asResults() }
        MapError(LoadError.network)
    }

    let elements = await messages.toArray()
    #expect(elements.count == 3)
    #expect(try elements[0].get() == "hello")
    #expect(try elements[1].get() == "world")
    guard case .failure(.network(let error)) = elements[2] else {
        Issue.record("the last element is \(elements[2]), not the stream's error")
        return
    }
    #expect(error is SocketClosed)
    // `DeferResult` makes a new stream for every run.
    #expect(await messages.toArray().count == 3)
}

@Test
func readmeChangingValues() async {
    let labels = Pipe<String, Never> {
        From(["1", "two", "3", "4", "5"])
        CompactMap { (text: String) in Int(text) }  // 1, 3, 4, 5
        Drop(1)  // 3, 4, 5
        Take(2)  // 3, 4
        Tap { (n: Int) in print("got", n) }  // prints 3 and 4
        Map { (n: Int) in "#\(n)" }  // "#3", "#4"
    }

    #expect(await labels.toResult() == .success(["#3", "#4"]))
}

@Test
func readmeHandlingFailures() async {
    let items = Pipe<String, FetchError> {
        From([1, 2, 3])
        FlatMap { (id: Int) -> Result<String, FetchError> in
            id == 2 ? .failure(.timeout) : .success("item \(id)")
        }
        TapError { (error: FetchError) in print("failed:", error) }
        FlatMapError { (error: FetchError) -> Result<String, FetchError> in
            error == .timeout ? .success("cached item") : .failure(error)
        }
    }

    #expect(await items.toResult() == .success(["item 1", "cached item", "item 3"]))

    let names = Pipe<String, Never> {
        FromResult(items)
        GetOrElse { (_: FetchError) in "unknown" }
    }

    let lines = Pipe<String, Never> {
        FromResult(items)
        Match(
            onSuccess: { (item: String) in "ok: \(item)" },
            onFailure: { (error: FetchError) in "error: \(error)" }
        )
    }

    #expect(await names.toResult() == .success(["item 1", "cached item", "item 3"]))
    #expect(
        await lines.toResult() == .success(["ok: item 1", "ok: cached item", "ok: item 3"])
    )
}

@Test
func readmeUntilFailure() async {
    let firstItems = Pipe<String, FetchError> {
        From([1, 2, 3])
        FlatMap { (id: Int) -> Result<String, FetchError> in
            id == 2 ? .failure(.notFound) : .success("item \(id)")
        }
        UntilFailure()
    }

    #expect(await firstItems.toArray() == [.success("item 1"), .failure(.notFound)])
    #expect(await firstItems.toResult() == .failure(.notFound))
}

@Test
func readmeTurningOneElementIntoMany() async {
    let repeated = Pipe<Int, Never> {
        From([1, 2, 3])
        FlatMapSequence { (n: Int) in Array(repeating: n, count: n) }
    }

    #expect(await repeated.toResult() == .success([1, 2, 2, 3, 3, 3]))
}

@Test
func readmeOpenPipes() async {
    let double = OpenPipe {
        From(Int.self)
        Filter { (n: Int) in n > 0 }
        Map { (n: Int) in n * 2 }
    }

    #expect(await double([1, -1, 2]).toResult() == .success([2, 4]))
    #expect(await double(1...3).toResult() == .success([2, 4, 6]))

    let tenTimes = OpenPipe {
        FromResult(Int.self, ParseError.self)
        Map { (n: Int) in n * 10 }
    }

    let input: [Result<Int, ParseError>] = [
        .success(1),
        .failure(.notANumber("x")),
    ]
    #expect(await tenTimes(input).toArray() == [.success(10), .failure(.notANumber("x"))])

    // `FromAsync(T.self)` works the same as `From(T.self)`.
    let plusOne = OpenPipe {
        FromAsync(Int.self)
        Map { (n: Int) in n + 1 }
    }
    #expect(await plusOne([1]).toResult() == .success([2]))
}

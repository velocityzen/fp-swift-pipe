@testable import FPPipe
import Testing

// With `ordered: true`, every concurrent stage gives results in source order. Element `n` waits
// `n × 5` ms, so the elements finish in the opposite order: without `ordered`, 1 would come out
// first. Elements that pass through without the closure keep their place too.

private enum E: Error, Equatable, Sendable {
    case failed(Int)
}

private let values = [5, 4, 3, 2, 1]

private func pause(_ n: Int) async {
    try? await Task.sleep(for: .milliseconds(n * 5))
}

/// Fails on the even values.
private func failEven(_ n: Int) -> Result<Int, E> {
    n.isMultiple(of: 2) ? .failure(.failed(n)) : .success(n)
}

private func failAll(_ n: Int) -> Result<Int, E> {
    .failure(.failed(n))
}

// MARK: - Success-side stages

@Test
func asyncMapOrderedKeepsFailuresInPlace() async {
    let pipe = Pipe<Int, E> {
        From(values)
        FlatMap(failEven)
        AsyncMap(concurrency: 5, ordered: true) { (n: Int) async -> Int in
            await pause(n)
            return n * 10
        }
    }

    #expect(
        await pipe.toArray() == [
            .success(50), .failure(.failed(4)), .success(30), .failure(.failed(2)), .success(10),
        ]
    )
}

@Test
func asyncFlatMapOrdered() async {
    let pipe = Pipe<Int, E> {
        From(values)
        AsyncFlatMap(concurrency: 5, ordered: true) { (n: Int) async -> Result<Int, E> in
            await pause(n)
            return failEven(n)
        }
    }

    #expect(
        await pipe.toArray() == [
            .success(5), .failure(.failed(4)), .success(3), .failure(.failed(2)), .success(1),
        ]
    )
}

@Test
func asyncFilterOrdered() async {
    let pipe = Pipe<Int, Never> {
        From(values)
        AsyncFilter(concurrency: 5, ordered: true) { (n: Int) async -> Bool in
            await pause(n)
            return !n.isMultiple(of: 2)
        }
    }

    #expect(await pipe.toResult() == .success([5, 3, 1]))
}

@Test
func asyncCompactMapOrdered() async {
    let pipe = Pipe<Int, Never> {
        From(values)
        AsyncCompactMap(concurrency: 5, ordered: true) { (n: Int) async -> Int? in
            await pause(n)
            return n.isMultiple(of: 2) ? nil : n * 10
        }
    }

    #expect(await pipe.toResult() == .success([50, 30, 10]))
}

// MARK: - Failure-side stages

@Test
func asyncFlatMapErrorOrdered() async {
    let pipe = Pipe<Int, E> {
        From(values)
        FlatMap(failAll)
        AsyncFlatMapError(concurrency: 5, ordered: true) { (error: E) async -> Result<Int, E> in
            guard case .failed(let n) = error else { return .failure(error) }
            await pause(n)
            return .success(n)
        }
    }

    #expect(await pipe.toResult() == .success(values))
}

@Test
func asyncGetOrElseOrdered() async {
    let pipe = Pipe<Int, Never> {
        From(values)
        FlatMap(failAll)
        AsyncGetOrElse(concurrency: 5, ordered: true) { (error: E) async -> Int in
            guard case .failed(let n) = error else { return 0 }
            await pause(n)
            return n
        }
    }

    #expect(await pipe.toResult() == .success(values))
}

@Test
func asyncAltOrdered() async {
    let pipe = Pipe<Int, E> {
        From(values)
        FlatMap(failEven)
        AsyncAlt(concurrency: 5, ordered: true) { () async -> Result<Int, E> in
            await pause(5)
            return .success(0)
        }
    }

    #expect(await pipe.toResult() == .success([5, 0, 3, 0, 1]))
}

@Test
func asyncMatchOrdered() async {
    let pipe = Pipe<String, Never> {
        From(values)
        FlatMap(failEven)
        AsyncMatch(
            concurrency: 5,
            ordered: true,
            onSuccess: { (n: Int) async -> String in
                await pause(n)
                return "s\(n)"
            },
            onFailure: { (error: E) async -> String in
                guard case .failed(let n) = error else { return "?" }
                await pause(n)
                return "f\(n)"
            }
        )
    }

    #expect(await pipe.toResult() == .success(["s5", "f4", "s3", "f2", "s1"]))
}

// MARK: - Pattern matching

@Test
func asyncMatchCasesOrdered() async {
    let value = Pipe<Int, E> {
        From(values)
        AsyncMatch(5, concurrency: 5, ordered: true) { () async -> Result<Int, E> in
            await pause(5)
            return .success(50)
        }
    }
    let type = Pipe<Int, E> {
        From(values)
        AsyncMatch(Int.self, concurrency: 5, ordered: true) { (n: Int) async -> Result<Int, E> in
            await pause(n)
            return .success(n * 10)
        }
    }
    let predicate = Pipe<Int, E> {
        From(values)
        AsyncMatch(
            where: { $0 > 3 },
            concurrency: 5,
            ordered: true,
            then: { (n: Int) async -> Result<Int, E> in
                await pause(n)
                return .success(n * 10)
            }
        )
    }
    let extract = Pipe<Int, E> {
        From(values)
        AsyncMatch(
            extract: { $0 > 3 ? $0 : nil },
            concurrency: 5,
            ordered: true,
            then: { (n: Int) async -> Result<Int, E> in
                await pause(n)
                return .success(n * 10)
            }
        )
    }

    #expect(await value.toResult() == .success([50, 4, 3, 2, 1]))
    #expect(await type.toResult() == .success([50, 40, 30, 20, 10]))
    #expect(await predicate.toResult() == .success([50, 40, 3, 2, 1]))
    #expect(await extract.toResult() == .success([50, 40, 3, 2, 1]))
}

@Test
func asyncMatchErrorCasesOrdered() async {
    let error = Pipe<Int, E> {
        From(values)
        FlatMap(failAll)
        AsyncMatchError(E.failed(5), concurrency: 5, ordered: true) { () async -> Result<Int, E> in
            await pause(5)
            return .success(5)
        }
    }
    let type = Pipe<Int, E> {
        From(values)
        FlatMap(failAll)
        AsyncMatchError(E.self, concurrency: 5, ordered: true) { (e: E) async -> Result<Int, E> in
            guard case .failed(let n) = e else { return .failure(e) }
            await pause(n)
            return .success(n)
        }
    }
    let predicate = Pipe<Int, E> {
        From(values)
        FlatMap(failAll)
        AsyncMatchError(
            where: { $0 == .failed(5) || $0 == .failed(4) },
            concurrency: 5,
            ordered: true,
            then: { (e: E) async -> Result<Int, E> in
                guard case .failed(let n) = e else { return .failure(e) }
                await pause(n)
                return .success(n)
            }
        )
    }
    let extract = Pipe<Int, E> {
        From(values)
        FlatMap(failAll)
        AsyncMatchError(
            extract: { (e: E) -> Int? in
                if case .failed(let n) = e, n > 3 { n } else { nil }
            },
            concurrency: 5,
            ordered: true,
            then: { (n: Int) async -> Result<Int, E> in
                await pause(n)
                return .success(n)
            }
        )
    }

    let firstTwo: [Result<Int, E>] = [
        .success(5), .success(4), .failure(.failed(3)), .failure(.failed(2)), .failure(.failed(1)),
    ]
    #expect(
        await error.toArray() == [
            .success(5), .failure(.failed(4)), .failure(.failed(3)), .failure(.failed(2)),
            .failure(.failed(1)),
        ]
    )
    #expect(await type.toResult() == .success(values))
    #expect(await predicate.toArray() == firstTwo)
    #expect(await extract.toArray() == firstTwo)
}

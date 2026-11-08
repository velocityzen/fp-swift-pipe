@testable import FPPipe
import Synchronization
import Testing

private enum AppError: Error, Equatable, Sendable { case bad, rejected(Int) }

private enum Response: Equatable, Sendable {
    case ok(Int)
    case unauthorized
    case redirect(String)

    var redirectTarget: String? {
        if case .redirect(let target) = self { target } else { nil }
    }
}

private protocol Animal: Sendable { var name: String { get } }
private struct Dog: Animal { let name: String }
private struct Cat: Animal { let name: String }

@Test
func asyncMatchValueReplacesOnlyEqualSuccesses() async {
    let pipe = Pipe<Response, AppError> {
        From([Response.ok(1), .unauthorized, .ok(2)])
        AsyncMatch(Response.unauthorized) { () async -> Result<Response, AppError> in
            try? await Task.sleep(nanoseconds: 1_000)
            return .success(.ok(401))
        }
    }

    #expect(await pipe.toResult() == .success([.ok(1), .ok(401), .ok(2)]))
}

@Test
func asyncMatchTypeHandsTheHandlerTheDowncastValue() async {
    let pipe = Pipe<String, AppError> {
        From([Dog(name: "rex") as any Animal, Cat(name: "tom")])
        AsyncMatch(Cat.self) { cat async -> Result<any Animal, AppError> in
            .success(Dog(name: "not-\(cat.name)"))
        }
        Map { (animal: any Animal) in animal.name }
    }

    #expect(await pipe.toResult() == .success(["rex", "not-tom"]))
}

@Test
func asyncMatchPredicateAndExtractReplaceTheirMatches() async {
    let pipe = Pipe<Response, AppError> {
        From([Response.ok(1), .ok(-5), .redirect("/home")])
        AsyncMatch(
            where: { (r: Response) in r == .ok(-5) },
            then: { _ async -> Result<Response, AppError> in
                .failure(.rejected(-5))
            }
        )
        AsyncMatch(extract: \Response.redirectTarget) {
            target async -> Result<Response, AppError> in
            .success(.ok(target.count))
        }
    }

    #expect(await pipe.toArray() == [.success(.ok(1)), .failure(.rejected(-5)), .success(.ok(5))])
}

@Test
func asyncMatchConcurrentRunsHandlersInParallel() async {
    let count = 4
    let rendezvous = Rendezvous(expected: count)
    let pipe = Pipe<Int, AppError> {
        From(0..<count)
        AsyncMatch(
            where: { (_: Int) in true },
            concurrency: count,
            then: { n async -> Result<Int, AppError> in
                await rendezvous.enter()
                return .success(n)
            }
        )
    }

    _ = await pipe.toResult()
    #expect(rendezvous.peak == count)
}

@Test
func asyncMatchConcurrentTestsEachElementOnceAndPassesTheRestThrough() async {
    // With concurrency > 1 the stage sorts elements on the reading side, so the predicate
    // runs exactly once per success — never again on the worker — and failures skip it.
    let tested = Mutex<[Int]>([])
    let pipe = Pipe<Int, AppError> {
        From(1...6)
        FlatMap { (n: Int) -> Result<Int, AppError> in n == 6 ? .failure(.bad) : .success(n) }
        AsyncMatch(
            where: { (n: Int) in
                tested.withLock { $0.append(n) }
                return n.isMultiple(of: 2)
            },
            concurrency: 3,
            then: { n async -> Result<Int, AppError> in
                try? await Task.sleep(nanoseconds: 1_000)
                return n == 4 ? .failure(.rejected(n)) : .success(n * 100)
            }
        )
    }

    let elements = await pipe.toArray()
    #expect(tested.withLock { $0.sorted() } == [1, 2, 3, 4, 5])
    #expect(elements.compactMap { try? $0.get() }.sorted() == [1, 3, 5, 200])
    #expect(elements.filter { (try? $0.get()) == nil }.count == 2)
}

@Test
func asyncMatchConcurrentExtractCallsTheExtractorOncePerElement() async {
    let calls = Mutex(0)
    let pipe = Pipe<Response, AppError> {
        From([Response.redirect("/a"), .ok(1), .redirect("/bb")])
        AsyncMatch(
            extract: { (r: Response) in
                calls.withLock { $0 += 1 }
                return r.redirectTarget
            },
            concurrency: 2,
            then: { target async -> Result<Response, AppError> in .success(.ok(target.count)) }
        )
    }

    let result = await pipe.toResult().map { $0.map { "\($0)" }.sorted() }
    #expect(result == .success(["ok(1)", "ok(2)", "ok(3)"]))
    #expect(calls.withLock { $0 } == 3)
}

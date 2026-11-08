@testable import FPPipe
import Synchronization
import Testing

private enum AppError: Error, Equatable, Sendable { case bad, unauthorized, rejected(Int) }

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
func matchValueReplacesOnlyEqualSuccesses() async {
    let pipe = Pipe<Response, AppError> {
        From([Response.ok(1), .unauthorized, .ok(2)])
        Match(Response.unauthorized) { .failure(AppError.unauthorized) }
    }

    let elements = await pipe.toArray()
    #expect(elements == [.success(.ok(1)), .failure(.unauthorized), .success(.ok(2))])
}

@Test
func matchValueCanSwapTheValue() async {
    let pipe = Pipe<Int, AppError> {
        From([0, 1, 0])
        Match(0) { Result<Int, AppError>.success(-1) }
    }

    let result = await pipe.toResult()
    #expect(result == .success([-1, 1, -1]))
}

@Test
func matchTypeHandsTheHandlerTheDowncastValue() async {
    let pipe = Pipe<String, AppError> {
        From([Dog(name: "rex") as any Animal, Cat(name: "tom"), Dog(name: "max")])
        Match(Cat.self) { cat -> Result<any Animal, AppError> in
            .success(Dog(name: "not-\(cat.name)"))
        }
        Map { (animal: any Animal) in animal.name }
    }

    let result = await pipe.toResult()
    #expect(result == .success(["rex", "not-tom", "max"]))
}

@Test
func matchPredicateReplacesAcceptedSuccesses() async {
    let pipe = Pipe<Int, AppError> {
        From([1, -2, 3, -4])
        Match(where: { (n: Int) in n < 0 }, then: { n in .failure(AppError.rejected(n)) })
    }

    let elements = await pipe.toArray()
    #expect(
        elements == [.success(1), .failure(.rejected(-2)), .success(3), .failure(.rejected(-4))]
    )
}

@Test
func matchExtractHandsTheHandlerThePayload() async {
    let pipe = Pipe<Response, AppError> {
        From([Response.ok(1), .redirect("/a"), .redirect("/b")])
        Match(extract: \Response.redirectTarget) { target -> Result<Response, AppError> in
            .success(.ok(target.count))
        }
    }

    let result = await pipe.toResult()
    #expect(result == .success([.ok(1), .ok(2), .ok(2)]))
}

@Test
func matchExtractClosureMatchesAnEnumCase() async {
    let pipe = Pipe<Response, AppError> {
        From([Response.ok(1), .ok(500), .unauthorized])
        Match(
            extract: { (r: Response) in if case .ok(let code) = r, code >= 500 { code } else { nil }
            },
            then: {
                code in .failure(AppError.rejected(code))
            }
        )
    }

    let elements = await pipe.toArray()
    #expect(elements == [.success(.ok(1)), .failure(.rejected(500)), .success(.unauthorized)])
}

@Test
func matchLeavesFailuresAloneAndNeverTestsThem() async {
    let tested = Mutex<[Int]>([])
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in n == 2 ? .failure(.bad) : .success(n) }
        Match(
            where: { (n: Int) in
                tested.withLock { $0.append(n) }
                return true
            },
            then: { n -> Result<Int, AppError> in .success(n * 10) }
        )
    }

    let elements = await pipe.toArray()
    #expect(elements == [.success(10), .failure(.bad), .success(30)])
    #expect(tested.withLock { $0 } == [1, 3])
}

@Test
func matchChainsLikeSwitchCases() async {
    // Successive `Match` stages act as the arms of a switch over the stream.
    let pipe = Pipe<String, AppError> {
        From([Response.ok(200), .unauthorized, .redirect("/home")])
        Match(Response.unauthorized) { .failure(AppError.unauthorized) }
        Match(extract: \Response.redirectTarget) { _ -> Result<Response, AppError> in
            .success(.ok(301))
        }
        Map { (response: Response) in "\(response)" }
    }

    let elements = await pipe.toArray()
    #expect(elements == [.success("ok(200)"), .failure(.unauthorized), .success("ok(301)")])
}

@Test
func matchComposesInIfWithoutElse() async {
    func make(_ strict: Bool) -> Pipe<Int, AppError> {
        Pipe<Int, AppError> {
            From([1, -1])
            FlatMap { (n: Int) -> Result<Int, AppError> in .success(n) }
            if strict {
                Match(-1) { .failure(AppError.bad) }
            }
        }
    }

    #expect(await make(true).toArray() == [.success(1), .failure(.bad)])
    #expect(await make(false).toArray() == [.success(1), .success(-1)])
}

@Test
func matchWorksInOpenPipe() async {
    let pipe = OpenPipe {
        From(Int.self)
        Match(0) { .failure(AppError.bad) }
    }

    #expect(await pipe([1, 0]).toArray() == [.success(1), .failure(.bad)])
}

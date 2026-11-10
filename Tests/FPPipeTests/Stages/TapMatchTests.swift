@testable import FPPipe
import Synchronization
import Testing

private enum AppError: Error, Equatable, Sendable { case bad }

private enum Event: Equatable, Sendable {
    case login(String)
    case logout

    var loggedInUser: String? {
        if case .login(let user) = self { user } else { nil }
    }
}

private protocol Animal: Sendable { var name: String { get } }
private struct Dog: Animal { let name: String }
private struct Cat: Animal { let name: String }

private let events: [Event] = [.login("ann"), .logout, .login("bob"), .logout]

@Test
func tapMatchValueRunsOnlyForEqualSuccesses() async {
    let count = Mutex(0)
    let pipe = Pipe<Event, Never> {
        From(events)
        TapMatch(Event.logout) { count.withLock { $0 += 1 } }
    }

    #expect(await pipe.toResult() == .success(events))
    #expect(count.withLock { $0 } == 2)
}

@Test
func tapMatchTypeSeesTheDowncastValue() async {
    let seen = Mutex<[String]>([])
    let pipe = Pipe<String, Never> {
        From([Dog(name: "rex") as any Animal, Cat(name: "tom"), Dog(name: "max")])
        TapMatch(Dog.self) { dog in seen.withLock { $0.append(dog.name) } }
        Map { (animal: any Animal) in animal.name }
    }

    #expect(await pipe.toResult() == .success(["rex", "tom", "max"]))
    #expect(seen.withLock { $0 } == ["rex", "max"])
}

@Test
func tapMatchTypeFitsAFailablePipe() async {
    // The type form works with any failure type.
    let seen = Mutex<[Int]>([])
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in n == 2 ? .failure(.bad) : .success(n) }
        TapMatch(Int.self) { n in seen.withLock { $0.append(n) } }
    }

    #expect(await pipe.toArray() == [.success(1), .failure(.bad), .success(3)])
    #expect(seen.withLock { $0 } == [1, 3])
}

@Test
func tapMatchPredicateRunsForAcceptedSuccesses() async {
    let seen = Mutex<[Int]>([])
    let pipe = Pipe<Int, Never> {
        From(1...6)
        TapMatch(
            where: { (n: Int) in n.isMultiple(of: 3) },
            then: { n in seen.withLock { $0.append(n) } }
        )
    }

    #expect(await pipe.toResult() == .success([1, 2, 3, 4, 5, 6]))
    #expect(seen.withLock { $0 } == [3, 6])
}

@Test
func tapMatchExtractRunsWithThePayload() async {
    let users = Mutex<[String]>([])
    let pipe = Pipe<Event, Never> {
        From(events)
        TapMatch(extract: \Event.loggedInUser) { user in users.withLock { $0.append(user) } }
    }

    #expect(await pipe.toResult() == .success(events))
    #expect(users.withLock { $0 } == ["ann", "bob"])
}

@Test
func tapMatchIgnoresFailures() async {
    let count = Mutex(0)
    let pipe = Pipe<Int, AppError> {
        From([0, 0])
        FlatMap { (_: Int) -> Result<Int, AppError> in .failure(.bad) }
        TapMatch(where: { (_: Int) in true }, then: { _ in count.withLock { $0 += 1 } })
    }

    #expect(await pipe.toArray() == [.failure(.bad), .failure(.bad)])
    #expect(count.withLock { $0 } == 0)
}

@Test
func tapMatchComposesInIfWithoutElse() async {
    let count = Mutex(0)
    func make(_ audit: Bool) -> Pipe<Event, Never> {
        Pipe<Event, Never> {
            From(events)
            if audit {
                TapMatch(Event.logout) { count.withLock { $0 += 1 } }
            }
            if audit {
                TapMatch(Dog.self) { _ in }
            }
        }
    }

    _ = await make(false).toResult()
    #expect(count.withLock { $0 } == 0)
    _ = await make(true).toResult()
    #expect(count.withLock { $0 } == 2)
}

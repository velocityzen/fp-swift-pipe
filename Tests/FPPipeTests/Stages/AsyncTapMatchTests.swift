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
func asyncTapMatchRunsEachFormForItsMatchesInOrder() async {
    let log = Mutex<[String]>([])
    let pipe = Pipe<Event, Never> {
        From(events)
        AsyncTapMatch(Event.logout) {
            try? await Task.sleep(nanoseconds: 1_000)
            log.withLock { $0.append("logout") }
        }
        AsyncTapMatch(extract: \Event.loggedInUser) { user async in
            log.withLock { $0.append("login:\(user)") }
        }
        AsyncTapMatch(
            where: { (e: Event) in e == .login("bob") },
            then: { _ async in
                log.withLock { $0.append("bob!") }
            }
        )
    }

    #expect(await pipe.toResult() == .success(events))
    #expect(log.withLock { $0 } == ["login:ann", "logout", "login:bob", "bob!", "logout"])
}

@Test
func asyncTapMatchTypeSeesTheDowncastValue() async {
    let seen = Mutex<[String]>([])
    let pipe = Pipe<String, AppError> {
        From([Dog(name: "rex") as any Animal, Cat(name: "tom"), Dog(name: "max")])
        FlatMap { (animal: any Animal) -> Result<any Animal, AppError> in .success(animal) }
        AsyncTapMatch(Dog.self) { dog async in seen.withLock { $0.append(dog.name) } }
        Map { (animal: any Animal) in animal.name }
    }

    #expect(await pipe.toResult() == .success(["rex", "tom", "max"]))
    #expect(seen.withLock { $0 } == ["rex", "max"])
}

@Test
func asyncTapMatchIgnoresFailures() async {
    let count = Mutex(0)
    let pipe = Pipe<Int, AppError> {
        From([0, 0])
        FlatMap { (_: Int) -> Result<Int, AppError> in .failure(.bad) }
        AsyncTapMatch(where: { (_: Int) in true }, then: { _ async in count.withLock { $0 += 1 } })
        AsyncTapMatch(Int.self) { _ async in count.withLock { $0 += 1 } }
    }

    #expect(await pipe.toArray() == [.failure(.bad), .failure(.bad)])
    #expect(count.withLock { $0 } == 0)
}

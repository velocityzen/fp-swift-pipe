@testable import FPPipe
import Testing

@Test
func takeLimitsElementCount() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4, 5])
        Take(3)
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 2, 3]))
}

private enum E: Error, Equatable, Sendable { case bad }

@Test
func takeZeroYieldsNothing() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        Take(0)
    }
    let result = await pipe.toResult()
    #expect(result == .success([]))
}

@Test
func takeBeyondLengthYieldsEverything() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2])
        Take(10)
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 2]))
}

@Test
func takeCountsFailuresAgainstTheBudget() async {
    let pipe = Pipe<Int, E> {
        From([1, -1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
        Take(2)
    }
    var seen: [Result<Int, E>] = []
    for await element in pipe { seen.append(element) }
    #expect(seen == [.success(1), .failure(.bad)])
}

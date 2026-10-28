@testable import FPPipe
import Testing

@Test
func dropSkipsLeadingElements() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4, 5])
        Drop(2)
    }
    let result = await pipe.toResult()
    #expect(result == .success([3, 4, 5]))
}

private enum E: Error, Equatable, Sendable { case bad }

@Test
func dropZeroYieldsEverything() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        Drop(0)
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 2, 3]))
}

@Test
func dropBeyondLengthYieldsNothing() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2])
        Drop(10)
    }
    let result = await pipe.toResult()
    #expect(result == .success([]))
}

@Test
func dropCountsFailuresAgainstTheBudget() async {
    let pipe = Pipe<Int, E> {
        From([-1, 1, 2])
        FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
        Drop(1)
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 2]))
}

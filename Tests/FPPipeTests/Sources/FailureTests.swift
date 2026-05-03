@testable import FPPipe
import Testing

private enum E: Error, Equatable { case bad }

@Test
func failureEmitsOneFailure() async {
    let result = await Pipe<Int, E> { Failure(E.bad, valueType: Int.self) }.toResult()
    #expect(result == .failure(.bad))
}

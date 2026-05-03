@testable import FPPipe
import Testing

private enum E: Error, Equatable { case bad }

@Test
func emptyEmitsNothing() async {
    let result = await Pipe<Int, E> {
        Empty(valueType: Int.self, failureType: E.self)
    }.toResult()
    #expect(result == .success([]))
}

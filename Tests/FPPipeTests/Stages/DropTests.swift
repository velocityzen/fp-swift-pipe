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

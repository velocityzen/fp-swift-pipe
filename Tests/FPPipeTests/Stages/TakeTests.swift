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

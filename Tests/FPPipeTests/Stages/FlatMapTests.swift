@testable import FPPipe
import Testing

private enum TestError: Error, Equatable { case negative }

@Test
func flatMapShortCircuitsOnFailure() async {
    let pipe = Pipe<Int, TestError> {
        From([1, -1, 2])
        FlatMap { (n: Int) -> Result<Int, TestError> in
            n < 0 ? .failure(.negative) : .success(n * 2)
        }
    }

    let result = await pipe.toResult()
    #expect(result == .failure(.negative))
}

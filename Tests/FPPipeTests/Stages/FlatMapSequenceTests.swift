@testable import FPPipe
import Testing

@Test
func flatMapSequenceExpandsEachSuccess() async {
    let pipe = Pipe<Int, Never> {
        From([2, 3])
        FlatMapSequence { (n: Int) in 0..<n }
    }
    let result = await pipe.toResult()
    #expect(result == .success([0, 1, 0, 1, 2]))
}

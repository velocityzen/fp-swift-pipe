@testable import PipelineKit
import Testing

@Test
func mapAppliesTransformToEverySuccess() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        Map { (n: Int) in n * 100 }
    }
    let result = await pipe.toResult()
    #expect(result == .success([100, 200, 300]))
}

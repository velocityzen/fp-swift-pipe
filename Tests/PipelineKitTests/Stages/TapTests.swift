@testable import PipelineKit
import Synchronization
import Testing

@Test
func tapObservesEverySuccess() async {
    let counter = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From([10, 20, 30])
        Tap { (n: Int) in counter.withLock { $0 += n } }
    }

    let result = await pipe.toResult()
    #expect(result == .success([10, 20, 30]))
    #expect(counter.withLock { $0 } == 60)
}

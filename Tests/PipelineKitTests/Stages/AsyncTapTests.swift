@testable import PipelineKit
import Synchronization
import Testing

@Test
func asyncTapObservesEverySuccess() async {
    let counter = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        AsyncTap { (n: Int) async in
            try? await Task.sleep(nanoseconds: 1_000)
            counter.withLock { $0 += n }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 2, 3]))
    #expect(counter.withLock { $0 } == 6)
}

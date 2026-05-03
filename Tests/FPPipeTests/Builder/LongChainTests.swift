@testable import FPPipe
import Testing

// Pins the result builder's compile-time behavior at a non-trivial chain length. If the
// builder's overload-resolution cliff moves (e.g. someone adds an overload that broadens
// the search space), the compiler will start to take noticeably longer or fail outright on
// this test. Currently 30 chained stages.
@Test
func longStageChainCompilesAndExecutes() async {
    let pipe = Pipe<Int, Never> {
        From([0])
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n + 1 }
    }
    let result = await pipe.toResult()
    #expect(result == .success([30]))
}

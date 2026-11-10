@testable import FPPipe
import Testing

// 30 stages. If a builder change makes type checking much slower, this test shows it by
// compiling slowly or not at all.
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

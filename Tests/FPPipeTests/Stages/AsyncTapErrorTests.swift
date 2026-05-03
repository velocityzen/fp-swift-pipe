@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable { case bad }

@Test
func asyncTapErrorObservesEveryFailure() async {
    let observed = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From([1, -1, 2, -2])
        FlatMap { (n: Int) -> Result<Int, E> in
            n < 0 ? .failure(.bad) : .success(n)
        }
        AsyncTapError { (_: E) async in
            try? await Task.sleep(nanoseconds: 1_000)
            observed.withLock { $0 += 1 }
        }
    }
    var seen: [Result<Int, E>] = []
    for await x in pipe {
        seen.append(x)
    }
    #expect(seen.count == 4)
    #expect(observed.withLock { $0 } == 2)
}

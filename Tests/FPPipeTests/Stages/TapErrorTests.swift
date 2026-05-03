@testable import FPPipe
import Synchronization
import Testing

private enum AppError: Error, Equatable { case empty }

@Test
func tapErrorObservesEveryFailure() async {
    let observed = Mutex<[AppError]>([])
    let pipe = Pipe<Int, AppError> {
        From([1, -1, 2, -2])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n < 0 ? .failure(.empty) : .success(n)
        }
        TapError { (e: AppError) in observed.withLock { $0.append(e) } }
    }

    // Drain by iterating manually so we observe both failures (toResult short-circuits).
    var seen: [Result<Int, AppError>] = []
    for await element in pipe {
        seen.append(element)
    }

    #expect(seen.count == 4)
    #expect(observed.withLock { $0 } == [.empty, .empty])
}

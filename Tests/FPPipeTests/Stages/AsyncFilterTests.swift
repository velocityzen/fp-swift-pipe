@testable import FPPipe
import FP
import Testing

private enum AppError: Error, Equatable { case bad }

@Test
func asyncFilterKeepsMatchingSuccessesAndPassesFailures() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3, 4, 5])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 3 ? .failure(.bad) : .success(n)
        }
        AsyncFilter { (n: Int) async in
            try? await Task.sleep(nanoseconds: 1_000)
            return n.isMultiple(of: 2)
        }
    }

    var observed: [Result<Int, AppError>] = []
    for await element in pipe {
        observed.append(element)
    }

    #expect(observed == [.success(2), .failure(.bad), .success(4)])
}

@Test
func asyncFilterConcurrentRetainsExpectedElements() async {
    let pipe = Pipe<Int, Never> {
        From(0..<20)
        AsyncFilter(concurrency: 8) { (n: Int) async in n.isMultiple(of: 3) }
    }

    let result = await pipe.toResult()
    let values = result.getOrElse([])
    #expect(Set(values) == Set([0, 3, 6, 9, 12, 15, 18]))
}

@Test
func asyncFilterConcurrentParallelizesWork() async {
    // Passes only if all `count` closures run at once. It measures no time, so slow CI is fine.
    let count = 5
    let rendezvous = Rendezvous(expected: count)
    let pipe = Pipe<Int, Never> {
        From(0..<count)
        AsyncFilter(concurrency: count) { (_: Int) async in
            await rendezvous.enter()
            return true
        }
    }

    _ = await pipe.toResult()
    #expect(rendezvous.peak == count)
}

@testable import FPPipe
import Testing

private enum AppError: Error, Equatable, Sendable { case bad }

@Test
func asyncMatchFoldsAsynchronously() async {
    let pipe = Pipe<String, Never> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 2 ? .failure(.bad) : .success(n)
        }
        AsyncMatch(
            onSuccess: { (n: Int) async -> String in
                try? await Task.sleep(nanoseconds: 1_000)
                return "ok=\(n)"
            },
            onFailure: { (e: AppError) async -> String in
                try? await Task.sleep(nanoseconds: 1_000)
                return "err=\(e)"
            },
        )
    }
    let result = await pipe.toResult()
    #expect(result == .success(["ok=1", "err=bad", "ok=3"]))
}

@Test
func asyncMatchConcurrentFoldsUnordered() async {
    // concurrency > 1 takes the unordered path: elements emit as they complete,
    // so compare contents rather than order.
    let pipe = Pipe<String, Never> {
        From([1, 2, 3, 4])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n.isMultiple(of: 2) ? .failure(.bad) : .success(n)
        }
        AsyncMatch(
            concurrency: 4,
            onSuccess: { (n: Int) async -> String in
                try? await Task.sleep(nanoseconds: 1_000)
                return "ok=\(n)"
            },
            onFailure: { (e: AppError) async -> String in
                try? await Task.sleep(nanoseconds: 1_000)
                return "err=\(e)"
            },
        )
    }
    let result = await pipe.toResult()
    #expect(result.map { $0.sorted() } == .success(["err=bad", "err=bad", "ok=1", "ok=3"]))
}

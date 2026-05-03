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

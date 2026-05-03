@testable import FPPipe
import Testing

private enum AppError: Error, Equatable, Sendable { case bad }

@Test
func matchFoldsBothChannelsIntoOneType() async {
    let pipe = Pipe<String, Never> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 2 ? .failure(.bad) : .success(n)
        }
        Match(
            onSuccess: { (n: Int) in "ok=\(n)" },
            onFailure: { (e: AppError) in "err=\(e)" },
        )
    }
    let result = await pipe.toResult()
    #expect(result == .success(["ok=1", "err=bad", "ok=3"]))
}

@Test
func matchOutputPipeCannotFail() async {
    // After Match, Failure is Never — pipeline always succeeds at the type level.
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 2 ? .failure(.bad) : .success(n)
        }
        Match(
            onSuccess: { (n: Int) in n * 10 },
            onFailure: { (_: AppError) in -1 },
        )
    }
    let result = await pipe.toResult()
    #expect(result == .success([10, -1, 30]))
}

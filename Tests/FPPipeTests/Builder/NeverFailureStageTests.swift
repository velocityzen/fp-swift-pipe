@testable import FPPipe
import Testing

private enum TestError: Error, Equatable { case bad }

// A stage that cannot fail, after stages that cannot fail, matches two builder overloads.
// Without a third, more specific one, these pipes fail with "ambiguous use of
// 'buildPartialBlock'".

@Test
func neverFailingFlatMapComposesAfterNeverSource() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, Never> in .success(n * 10) }
        FlatMap { (n: Int) -> Result<String, Never> in .success("v=\(n)") }
        Map { (s: String) in s.count }
    }

    #expect(await pipe.toResult() == .success([4, 4, 4]))
}

@Test
func neverFailingAsyncStagesComposeAfterNeverSource() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        AsyncFlatMap { (n: Int) async -> Result<Int, Never> in .success(n + 1) }
        AsyncFlatMap(concurrency: 2, ordered: true) { (n: Int) async -> Result<Int, Never> in
            .success(n * 2)
        }
    }

    #expect(await pipe.toResult() == .success([4, 6, 8]))
}

@Test
func neverFailingMatchComposesAfterNeverSource() async {
    let pipe = Pipe<Int, Never> {
        From([0, 1, 0])
        Match(0) { Result<Int, Never>.success(-1) }
        AsyncMatch(
            where: { (n: Int) in n == 1 },
            then: { n async -> Result<Int, Never> in .success(n * 100) }
        )
    }

    #expect(await pipe.toResult() == .success([-1, 100, -1]))
}

@Test
func neverFailingStageComposesAfterRecoveryToNever() async {
    // `GetOrElse` sets the failure type to `Never`.
    let pipe = Pipe<Int, Never> {
        From([1, 2])
        FlatMap { (n: Int) -> Result<Int, TestError> in n == 2 ? .failure(.bad) : .success(n) }
        GetOrElse { (_: TestError) in 0 }
        FlatMap { (n: Int) -> Result<Int, Never> in .success(n + 1) }
    }

    #expect(await pipe.toResult() == .success([2, 1]))
}

@Test
func neverFailingStageStillWidensIntoALaterFailureChannel() async {
    // A later stage that can fail must still set the failure type.
    let pipe = Pipe<Int, TestError> {
        From([1, 2])
        FlatMap { (n: Int) -> Result<Int, Never> in .success(n) }
        FlatMap { (n: Int) -> Result<Int, TestError> in n == 2 ? .failure(.bad) : .success(n) }
    }

    #expect(await pipe.toArray() == [.success(1), .failure(.bad)])
}

@Test
func neverFailingStageComposesInEveryBranchForm() async {
    func make(_ flag: Bool) -> Pipe<Int, Never> {
        Pipe<Int, Never> {
            From([1, 2])
            if flag {
                FlatMap { (n: Int) -> Result<Int, Never> in .success(n * 10) }
            } else {
                FlatMap { (n: Int) -> Result<Int, Never> in .success(n * 100) }
            }
            if flag {
                FlatMap { (n: Int) -> Result<Int, Never> in .success(n + 1) }
            }
        }
    }

    #expect(await make(true).toResult() == .success([11, 21]))
    #expect(await make(false).toResult() == .success([100, 200]))
}

@Test
func neverFailingStageComposesInOpenPipe() async {
    let pipe = OpenPipe {
        From(Int.self)
        FlatMap { (n: Int) -> Result<Int, Never> in .success(n * 2) }
        Match(4) { Result<Int, Never>.success(0) }
    }

    #expect(await pipe([1, 2, 3]).toResult() == .success([2, 0, 6]))
}

@Test
func neverFailingStageComposesInOpenPipeBranches() async {
    func make(_ flag: Bool) -> OpenPipe<Int, Int, Never> {
        OpenPipe {
            From(Int.self)
            if flag {
                FlatMap { (n: Int) -> Result<Int, Never> in .success(-n) }
            }
        }
    }

    #expect(await make(true)([1, 2]).toResult() == .success([-1, -2]))
    #expect(await make(false)([1, 2]).toResult() == .success([1, 2]))
}

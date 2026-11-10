@testable import FPPipe
import Testing

private enum TestError: Error, Equatable { case bad }
private enum WrappedError: Error, Equatable { case wrapped }

/// Types flow through `Map` and `FlatMap` with no extra type annotations.
@Test
func builderComposesHeterogeneousStages() async {
    let pipe = Pipe<String, TestError> {
        From([1, 2, 3])
        Map { (n: Int) in n + 1 }
        FlatMap { (n: Int) -> Result<String, TestError> in .success("v=\(n)") }
    }

    let result = await pipe.toResult()
    #expect(result == .success(["v=2", "v=3", "v=4"]))
}

/// A second stage with the same fixed failure type uses the plain combine overload.
@Test
func builderChainsFailureFixedStagesOnBoundChannel() async {
    let pipe = Pipe<Int, TestError> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, TestError> in .success(n * 10) }
        FlatMap { (n: Int) -> Result<Int, TestError> in
            n == 20 ? .failure(.bad) : .success(n + 1)
        }
    }

    let result = await pipe.toResult()
    #expect(result == .failure(.bad))
}

/// `MapError` after a source that cannot fail sets the failure type. Its closure never runs.
@Test
func builderWidensNeverFailureIntoPolyValueStage() async {
    let pipe = Pipe<Int, TestError> {
        From([1, 2, 3])
        MapError { (e: Never) -> TestError in switch e {} }
        FlatMap { (n: Int) -> Result<Int, TestError> in .success(n + 1) }
    }

    let result = await pipe.toResult()
    #expect(result == .success([2, 3, 4]))
}

/// The `OpenPipe` version of `builderChainsFailureFixedStagesOnBoundChannel`.
@Test
func openPipeChainsFailureFixedStagesOnBoundChannel() async {
    let pipe = OpenPipe {
        From(Int.self)
        FlatMap { (n: Int) -> Result<Int, TestError> in .success(n * 10) }
        FlatMap { (n: Int) -> Result<Int, TestError> in
            n == 20 ? .failure(.bad) : .success(n + 1)
        }
    }

    let result = await pipe([1, 2, 3]).toResult()
    #expect(result == .failure(.bad))
}

/// In an `OpenPipe`, `MapError` changes a failure type that is already set.
@Test
func openPipeTransformsBoundFailureChannel() async {
    let pipe = OpenPipe {
        From(Int.self)
        FlatMap { (n: Int) -> Result<Int, TestError> in
            n < 0 ? .failure(.bad) : .success(n)
        }
        MapError { (_: TestError) -> WrappedError in .wrapped }
    }

    let result = await pipe([1, -1, 2]).toResult()
    #expect(result == .failure(.wrapped))
}

/// The `OpenPipe` version of `builderWidensNeverFailureIntoPolyValueStage`.
@Test
func openPipeWidensNeverFailureIntoPolyValueStage() async {
    let pipe = OpenPipe {
        From(Int.self)
        MapError { (e: Never) -> TestError in switch e {} }
        FlatMap { (n: Int) -> Result<Int, TestError> in .success(n + 1) }
    }

    let result = await pipe([1, 2, 3]).toResult()
    #expect(result == .success([2, 3, 4]))
}

@testable import FPPipe
import Testing

private enum TestError: Error, Equatable { case bad }
private enum WrappedError: Error, Equatable { case wrapped }

/// The builder threads types through heterogeneous stages: source → Map (Int→Int) →
/// FlatMap (Int→Result<String, F>). Verifies that `buildPartialBlock` overload
/// resolution + `Never`-widening compose without explicit annotations.
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

/// With the failure channel already bound (non-`Never`), a second failure-fixed stage
/// resolves to the matching combine overload instead of the `Never`-widening one.
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

/// `MapError` straight after a non-failing source rebinds the failure channel from
/// `Never` so failure-fixed stages can follow; the transform itself can never run.
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

/// Open-pipe mirror of the bound-channel combine: an accumulated `OpenPipe` with
/// failure `F` followed by a stage fixed to the same `F`.
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

/// Open-pipe mirror of the value-polymorphic failure transform on a bound channel.
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

/// Open-pipe mirror of the `Never`-widening overload for value-polymorphic stages.
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

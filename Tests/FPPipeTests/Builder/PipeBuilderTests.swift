@testable import FPPipe
import Testing

private enum TestError: Error, Equatable { case bad }

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

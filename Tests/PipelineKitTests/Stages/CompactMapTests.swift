@testable import PipelineKit
import Testing

@Test
func compactMapDropsNils() async {
    let pipe = Pipe<Int, Never> {
        From(["1", "two", "3", "four", "5"])
        CompactMap { Int($0) }
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 3, 5]))
}

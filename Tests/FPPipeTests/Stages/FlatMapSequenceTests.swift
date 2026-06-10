@testable import FPPipe
import Testing

private enum E: Error, Equatable { case bad }

@Test
func flatMapSequenceExpandsEachSuccess() async {
    let pipe = Pipe<Int, Never> {
        From([2, 3])
        FlatMapSequence { (n: Int) in 0..<n }
    }
    let result = await pipe.toResult()
    #expect(result == .success([0, 1, 0, 1, 2]))
}

@Test
func flatMapSequenceWithEmptyInnerEmitsNothing() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        FlatMapSequence { (_: Int) in [Int]() }
    }
    let result = await pipe.toResult()
    #expect(result == .success([]))
}

@Test
func flatMapSequenceForwardsUpstreamFailures() async {
    let pipe = Pipe<Int, E> {
        From([2, -1, 2])
        FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
        FlatMapSequence { (n: Int) in 0..<n }
    }

    var seen: [Result<Int, E>] = []
    for await x in pipe {
        seen.append(x)
    }
    // The failure passes through in source position; the transform never sees it.
    #expect(seen == [.success(0), .success(1), .failure(.bad), .success(0), .success(1)])
}

@Test
func flatMapSequenceLargeInnerExpansionStopsOnEarlyBreak() async {
    // One element fans out into 1M inner elements; the consumer takes 5 and stops.
    // The mid-expansion cancellation check tears the producer down instead of
    // expanding the rest — the sink returns with just the prefix.
    let pipe = Pipe<Int, Never> {
        From([1_000_000])
        FlatMapSequence { (n: Int) in 0..<n }
        Take(5)
    }
    let result = await pipe.toResult()
    #expect(result == .success([0, 1, 2, 3, 4]))
}

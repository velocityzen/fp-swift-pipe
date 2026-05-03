@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable, Sendable { case bad }

// MARK: - if / else

@Test
func ifElseSelectsBranchAtBuildTime() async {
    let double = true
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        if double {
            Map { (n: Int) in n * 2 }
        } else {
            Map { (n: Int) in n + 1 }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([2, 4, 6]))
}

@Test
func ifElseElseBranchTaken() async {
    let double = false
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        if double {
            Map { (n: Int) in n * 2 }
        } else {
            Map { (n: Int) in n + 1 }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([2, 3, 4]))
}

// MARK: - switch

@Test
func switchPicksMatchingCase() async {
    enum Mode { case a, b, c }
    let mode = Mode.b
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4])
        switch mode {
            case .a: Filter { (n: Int) in n > 2 }
            case .b: Filter { (n: Int) in n.isMultiple(of: 2) }
            case .c: Filter { (_: Int) in true }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([2, 4]))
}

// MARK: - if (without else)

@Test
func ifWithoutElseIncludesStageWhenTrue() async {
    let logging = true
    let seen = AsyncStream<Int>.makeStream()
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        if logging {
            Tap { (n: Int) in seen.continuation.yield(n) }
        }
    }
    let result = await pipe.toResult()
    seen.continuation.finish()

    var observed: [Int] = []
    for await n in seen.stream { observed.append(n) }
    #expect(result == .success([1, 2, 3]))
    #expect(observed == [1, 2, 3])
}

@Test
func ifWithoutElseSkipsStageWhenFalse() async {
    let logging = false
    let counter = Mutex<Int>(0)
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        if logging {
            Tap { (_: Int) in counter.withLock { $0 += 1 } }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 2, 3]))
    #expect(counter.withLock { $0 } == 0)
}

@Test
func ifWithoutElseAllowsForwardingStage() async {
    let dropFirst = true
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4, 5])
        if dropFirst {
            Drop(2)
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([3, 4, 5]))
}

@Test
func ifWithoutElseAllowsTypePreservingFilter() async {
    let dropOdds = true
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4])
        if dropOdds {
            Filter { (n: Int) in n.isMultiple(of: 2) }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([2, 4]))
}

// MARK: - OptionalStage absent-branch (one per overload protocol)

@Test
func ifWithoutElseAbsentForwardingStageIsIdentity() async {
    let dropFirst = false
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4, 5])
        if dropFirst {
            Drop(2)  // PipeForwardingStage — absent path returns accumulated unchanged
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 2, 3, 4, 5]))
}

@Test
func ifWithoutElseAbsentPolyStageIsIdentity() async {
    let dropOdds = false
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3, 4])
        if dropOdds {
            Filter { (n: Int) in n.isMultiple(of: 2) }  // PipePolyStage<Int, Int>
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 2, 3, 4]))
}

@Test
func ifWithoutElseAbsentPolyValueStageIsIdentity() async {
    let observe = false
    let pipe = Pipe<Int, E> {
        From([1, -1, 2])
        FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
        if observe {
            TapError { (_: E) in }  // PipePolyValueStage<E, E>
        }
    }
    var seen: [Result<Int, E>] = []
    for await x in pipe { seen.append(x) }
    #expect(seen == [.success(1), .failure(.bad), .success(2)])
}

@Test
func ifWithoutElseInsideOpenPipe() async {
    let dropFirst = true
    let pipe = OpenPipe {
        From(Int.self)
        if dropFirst {
            Drop(1)
        }
    }
    let result = await pipe([1, 2, 3]).toResult()
    #expect(result == .success([2, 3]))
}

// MARK: - Open-pipe if/else

@Test
func ifElseWorksInsideOpenPipe() async {
    let plus = true
    let pipe = OpenPipe {
        From(Int.self)
        if plus {
            Map { (n: Int) in n + 1 }
        } else {
            Map { (n: Int) in n - 1 }
        }
    }
    let result = await pipe([1, 2, 3]).toResult()
    #expect(result == .success([2, 3, 4]))
}

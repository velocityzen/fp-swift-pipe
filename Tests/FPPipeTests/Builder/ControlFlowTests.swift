@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable, Sendable { case bad }
private enum E2: Error, Equatable, Sendable { case wrapped, other }

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
func ifWithoutElsePresentPolyValueStageObserves() async {
    let observe = true
    let counter = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From([1, -1, 2])
        FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
        if observe {
            TapError { (_: E) in counter.withLock { $0 += 1 } }  // present path
        }
    }
    var seen: [Result<Int, E>] = []
    for await x in pipe { seen.append(x) }
    #expect(seen == [.success(1), .failure(.bad), .success(2)])
    #expect(counter.withLock { $0 } == 1)
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

@Test
func ifWithoutElseInsideOpenPipeWithPolyStage() async {
    func make(_ evensOnly: Bool) -> OpenPipe<Int, Int, Never> {
        OpenPipe {
            From(Int.self)
            if evensOnly {
                Filter { (n: Int) in n.isMultiple(of: 2) }  // PipePolyStage<Int, Int>
            }
        }
    }
    let filtered = await make(true)([1, 2, 3, 4]).toResult()
    let passthrough = await make(false)([1, 2, 3, 4]).toResult()
    #expect(filtered == .success([2, 4]))
    #expect(passthrough == .success([1, 2, 3, 4]))
}

@Test
func ifWithoutElseInsideOpenPipeWithPolyValueStage() async {
    let counter = Mutex<Int>(0)
    func make(_ observe: Bool) -> OpenPipe<Int, Int, E> {
        OpenPipe {
            From(Int.self)
            FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
            if observe {
                TapError { (_: E) in counter.withLock { $0 += 1 } }  // PipePolyValueStage<E, E>
            }
        }
    }
    var seen: [Result<Int, E>] = []
    for await x in make(true)([1, -1, 2]) { seen.append(x) }
    #expect(seen == [.success(1), .failure(.bad), .success(2)])
    #expect(counter.withLock { $0 } == 1)

    let passthrough = await make(false)([3, 4]).toResult()
    #expect(passthrough == .success([3, 4]))
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

// MARK: - Branch bodies per stage shape
//
// An `if`/`else` branch whose body is a single stage routes through the stage-only
// `buildPartialBlock(first:)` for that stage's protocol — one test per shape.

@Test
func ifElseWithFailureFixedStageBranches() async {
    let strict = true
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        if strict {
            FlatMap { (n: Int) -> Result<Int, E> in n > 2 ? .failure(.bad) : .success(n) }
        } else {
            FlatMap { (n: Int) -> Result<Int, E> in .success(n) }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .failure(.bad))
}

@Test
func ifElseWithPolyValueStageBranches() async {
    let wrap = true
    let pipe = Pipe<Int, E2> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in n == 2 ? .failure(.bad) : .success(n) }
        if wrap {
            MapError { (_: E) -> E2 in .wrapped }
        } else {
            MapError { (_: E) -> E2 in .other }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .failure(.wrapped))
}

@Test
func ifElseWithFlatErrorStageBranches() async {
    let recover = true
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in n == 2 ? .failure(.bad) : .success(n) }
        if recover {
            FlatMapError { (_: E) -> Result<Int, E> in .success(99) }
        } else {
            FlatMapError { (_: E) -> Result<Int, E> in .failure(.bad) }
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success([1, 99, 3]))
}

@Test
func ifElseWithFoldStageBranches() async {
    let verbose = true
    let pipe = Pipe<String, Never> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in n == 2 ? .failure(.bad) : .success(n) }
        if verbose {
            Match(
                onSuccess: { (n: Int) in "ok=\(n)" },
                onFailure: { (_: E) in "err" },
            )
        } else {
            Match(
                onSuccess: { (n: Int) in "\(n)" },
                onFailure: { (_: E) in "?" },
            )
        }
    }
    let result = await pipe.toResult()
    #expect(result == .success(["ok=1", "err", "ok=3"]))
}

@testable import FPPipe
import Synchronization
import Testing

private enum E: Error, Equatable { case bad }
private enum NetError: Error, Equatable { case timeout }
private enum AppError: Error, Equatable { case network }

// MARK: - Re-iterability

/// A pipeline value can be iterated multiple times; each iteration is independent.
@Test
func pipelineIsReiterable() async {
    let pipe = Pipe<Int, Never> {
        From([1, 2, 3])
        Map { (n: Int) in n * 10 }
    }
    let a = await pipe.toResult()
    let b = await pipe.toResult()
    #expect(a == b)
    #expect(a == .success([10, 20, 30]))
}

// MARK: - Functor laws

/// Identity: `p |> Map { $0 } == p`.
@Test
func functorIdentity() async {
    let withId = Pipe<Int, Never> {
        From([1, 2, 3])
        Map { (n: Int) in n }
    }
    let plain = Pipe<Int, Never> { From([1, 2, 3]) }

    let a = await withId.toResult()
    let b = await plain.toResult()
    #expect(a == b)
}

/// Composition: `p |> Map(f) |> Map(g) == p |> Map(g ∘ f)`.
@Test
func functorComposition() async {
    let f: @Sendable (Int) -> Int = { $0 * 2 }
    let g: @Sendable (Int) -> Int = { $0 + 1 }

    let split = Pipe<Int, Never> {
        From([1, 2, 3])
        Map(f)
        Map(g)
    }
    let fused = Pipe<Int, Never> {
        From([1, 2, 3])
        Map { (n: Int) in g(f(n)) }
    }

    let a = await split.toResult()
    let b = await fused.toResult()
    #expect(a == b)
}

// MARK: - Bifunctor laws (over Failure)

/// Failure identity: `p |> MapError { $0 } == p`.
@Test
func bifunctorIdentityOverFailure() async {
    let withId = Pipe<Int, E> {
        From([1, -1, 2])
        FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
        MapError { (e: E) in e }
    }
    let plain = Pipe<Int, E> {
        From([1, -1, 2])
        FlatMap { (n: Int) -> Result<Int, E> in n < 0 ? .failure(.bad) : .success(n) }
    }

    var a: [Result<Int, E>] = []
    var b: [Result<Int, E>] = []
    for await x in withId {
        a.append(x)
    }
    for await x in plain {
        b.append(x)
    }
    #expect(a == b)
}

// MARK: - Short-circuit invariant

/// On `.failure`, downstream success-side closures must not run.
@Test
func failuresShortCircuitSuccessSideClosures() async {
    let mapHits = Mutex<Int>(0)
    let pipe = Pipe<Int, E> {
        From([1, 2, 3])
        FlatMap { (n: Int) -> Result<Int, E> in
            n == 2 ? .failure(.bad) : .success(n)
        }
        Map { (n: Int) -> Int in
            mapHits.withLock { $0 += 1 }
            return n
        }
    }

    var observed: [Result<Int, E>] = []
    for await x in pipe {
        observed.append(x)
    }

    // Map's closure ran for the two successes only — never for the failure.
    #expect(mapHits.withLock { $0 } == 2)
    #expect(observed == [.success(1), .failure(.bad), .success(3)])
}

// MARK: - Error-channel composition

/// A failure introduced mid-pipeline is re-typed by `MapError`, observed by `TapError`,
/// and selectively recovered by `FlatMapError` — all per element, successes untouched.
@Test
func errorFlowsThroughRetypeObserveRecoverChain() async {
    let observed = Mutex<Int>(0)
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3, 4])
        FlatMap { (n: Int) -> Result<Int, NetError> in
            n.isMultiple(of: 2) ? .failure(.timeout) : .success(n)
        }
        MapError { (_: NetError) in AppError.network }
        TapError { (_: AppError) in observed.withLock { $0 += 1 } }
        FlatMapError { (e: AppError) -> Result<Int, AppError> in
            e == .network ? .success(0) : .failure(e)
        }
    }

    var seen: [Result<Int, AppError>] = []
    for await x in pipe {
        seen.append(x)
    }

    #expect(seen == [.success(1), .success(0), .success(3), .success(0)])
    #expect(observed.withLock { $0 } == 2)
}

// MARK: - Associativity (composition is associative by construction)

/// `(source → a → b) → c` produces the same elements as `source → a → (b → c)`.
@Test
func compositionIsAssociative() async {
    let left = Pipe<Int, Never> {
        From([1, 2, 3])
        Map { (n: Int) in n + 1 }
        Map { (n: Int) in n * 2 }
        Map { (n: Int) in n - 3 }
    }
    let right = Pipe<Int, Never> {
        From([1, 2, 3])
        Map { (n: Int) in (n + 1) * 2 }
        Map { (n: Int) in n - 3 }
    }
    let a = await left.toResult()
    let b = await right.toResult()
    #expect(a == b)
}

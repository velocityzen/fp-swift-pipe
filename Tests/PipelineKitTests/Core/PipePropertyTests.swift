@testable import PipelineKit
import Testing

private enum E: Error, Equatable, Sendable { case bad }

// Property-style tests: verify functor / bifunctor / monad-ish laws across many random
// inputs. Not full SwiftCheck — just a parametric loop with a fixed seed so failures are
// reproducible. Each `@Test(arguments:)` runs the body once per input; `swift test` reports
// each as a separate case and the seed below makes the suite deterministic.

private func randomInts(seed: UInt64, count: Int, range: ClosedRange<Int>) -> [Int] {
    let rng = SystemRandomNumberGenerator()
    _ = rng  // silence unused warning under conditional builds
    var generator = LCG(seed: seed)
    return (0..<count).map { _ in Int.random(in: range, using: &generator) }
}

/// Tiny deterministic PRNG so test cases reproduce when given the same seed.
private struct LCG: RandomNumberGenerator {
    private var state: UInt64
    init(seed: UInt64) { self.state = seed == 0 ? 1 : seed }
    mutating func next() -> UInt64 {
        state = state &* 6_364_136_223_846_793_005 &+ 1_442_695_040_888_963_407
        return state
    }
}

private let seeds: [UInt64] = [1, 7, 42, 1024, 0xDEAD_BEEF, 9_999_999]

// MARK: - Functor identity: p |> Map { $0 } == p

@Test(arguments: seeds)
func functorIdentityHoldsForRandomInputs(seed: UInt64) async {
    let input = randomInts(seed: seed, count: 50, range: -1000...1000)
    let withId = Pipe<Int, Never> {
        From(input)
        Map { (n: Int) in n }
    }
    let plain = Pipe<Int, Never> { From(input) }
    let a = await withId.toResult()
    let b = await plain.toResult()
    #expect(a == b)
}

// MARK: - Functor composition: p |> Map(f) |> Map(g) == p |> Map(g ∘ f)

@Test(arguments: seeds)
func functorCompositionHoldsForRandomInputs(seed: UInt64) async {
    let input = randomInts(seed: seed, count: 30, range: -100...100)
    let f: @Sendable (Int) -> Int = { $0 &* 3 &+ 7 }
    let g: @Sendable (Int) -> Int = { $0 &- 11 }

    let split = Pipe<Int, Never> {
        From(input)
        Map(f)
        Map(g)
    }
    let fused = Pipe<Int, Never> {
        From(input)
        Map { (n: Int) in g(f(n)) }
    }

    let a = await split.toResult()
    let b = await fused.toResult()
    #expect(a == b)
}

// MARK: - Bifunctor identity over Failure: MapError { $0 } is identity

@Test(arguments: seeds)
func bifunctorIdentityOverFailureHoldsForRandomInputs(seed: UInt64) async {
    let input = randomInts(seed: seed, count: 30, range: -50...50)
    let lift: @Sendable (Int) -> Result<Int, E> = { n in n < 0 ? .failure(.bad) : .success(n) }

    let withId = Pipe<Int, E> {
        From(input)
        FlatMap(lift)
        MapError { (e: E) in e }
    }
    let plain = Pipe<Int, E> {
        From(input)
        FlatMap(lift)
    }

    var a: [Result<Int, E>] = []
    var b: [Result<Int, E>] = []
    for await x in withId { a.append(x) }
    for await x in plain { b.append(x) }
    #expect(a == b)
}

// MARK: - Filter idempotence: filter(p) ∘ filter(p) == filter(p)

@Test(arguments: seeds)
func filterIdempotenceHoldsForRandomInputs(seed: UInt64) async {
    let input = randomInts(seed: seed, count: 40, range: -100...100)
    let predicate: @Sendable (Int) -> Bool = { $0.isMultiple(of: 2) }

    let once = Pipe<Int, Never> {
        From(input)
        Filter(predicate)
    }
    let twice = Pipe<Int, Never> {
        From(input)
        Filter(predicate)
        Filter(predicate)
    }
    let a = await once.toResult()
    let b = await twice.toResult()
    #expect(a == b)
}

// MARK: - FlatMap left identity: From([x]) |> FlatMap(f) == f(x)

@Test(arguments: seeds)
func flatMapLeftIdentityHoldsForRandomInputs(seed: UInt64) async {
    let input = randomInts(seed: seed, count: 1, range: -100...100)
    let x = input[0]
    let f: @Sendable (Int) -> Result<Int, E> = { n in
        n.isMultiple(of: 2) ? .success(n * 10) : .failure(.bad)
    }

    let pipe = Pipe<Int, E> {
        From([x])
        FlatMap(f)
    }
    let result = await pipe.toResult()
    let expected: Result<[Int], E> = f(x).map { [$0] }
    #expect(result == expected)
}

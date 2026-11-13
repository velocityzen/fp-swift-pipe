// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import HegelTesting

/// What one pipe is made of, drawn once per test case, so pipes differ in
/// kind and not only in detail. A fixed mix gives every pipe the same macro
/// behavior: with each element a coin flip, an all-success input of length
/// `n` comes up once in `2ⁿ` pipes, and a pipe of only concurrent maps
/// almost never.
struct PipePolicy: Sendable, CustomStringConvertible {
    /// How often an input element is a failure.
    var failureRate: Double
    /// The ops the stages use, as indices into `PipeGen.ops`, or nil for
    /// every op.
    var ops: [Int]?
    /// How often a stage that has an async version uses it.
    var asyncRate: Double
    /// Every concurrent stage is ordered, with a few closures and a
    /// `bufferLimit` below them, so the limit holds closures back. Any
    /// concurrent stage can be like that, but only one in 50 or so is.
    var tight: Bool

    var description: String {
        let ops = self.ops.map { "\($0)" } ?? "all"
        return "failures \(failureRate), ops \(ops), async \(asyncRate), tight \(tight)"
    }
}

enum PipeGen {
    /// Small values usually, so patterns match often; any `Int` sometimes.
    static let value: Gen<Int> = .oneOf(.int(in: 0...30), .int(), .element(of: [.max, .min, -1]))

    /// Half failures first, so a failing pipe shrinks toward the mix; then
    /// none, rare, mostly and only failures.
    static let failureRate: Gen<Double> = .element(of: [0.5, 0.0, 0.05, 0.95, 1.0])

    static func element(failureRate: Double) -> Gen<Element> {
        Gen.bool(probability: failureRate).flatMap { fails in
            fails
                ? value.map { Element.failure(Fail(code: $0)) } : value.map { Element.success($0) }
        }
    }

    /// A few elements usually; sometimes many. Hegel keeps arrays short
    /// unless told otherwise, so the long ones get a minimum length.
    static func input(failureRate: Double) -> Gen<[Element]> {
        let element = element(failureRate: failureRate)
        return .oneOf(.array(of: element), .array(of: element, count: 50...Int.max))
    }

    /// Concurrent: more than one at once, in either order.
    static let parallel: Gen<Concurrency> = Hegel.zip(
        .oneOf(.int(in: 2...6), .int(in: 2...Int.max), .just(.max)),
        .bool(),
        .oneOf(.just(Int.max), .int(in: 0...4), .int(in: 0...Int.max))
    ).map {
        Concurrency(concurrency: $0, ordered: $1, bufferLimit: $2)
    }

    static let concurrency: Gen<Concurrency> = .oneOf(.just(.sequential), parallel)

    /// Ordered, with `bufferLimit` below `concurrency - 1`, so it lowers how
    /// many closures run.
    static let tight: Gen<Concurrency> = Hegel.zip(.int(in: 2...6), .int(in: 0...2)).map {
        Concurrency(concurrency: $0, ordered: true, bufferLimit: min($1, $0 - 2))
    }

    /// Small moduli match often; any `Int` is valid, 0 and negatives too.
    static let modulus: Gen<Int> = .oneOf(.int(in: -4...4), .int(), .element(of: [.min, .max]))
    static let small: Gen<Int> = .oneOf(.int(in: 0...5), .int())
    /// `Take` and `Drop` need at least 0.
    static let count: Gen<Int> = .oneOf(.int(in: 0...6), .int(in: 0...Int.max), .just(.max))
    /// At least 1.
    static let bufferSize: Gen<Int> = .oneOf(.int(in: 1...4), .int(in: 1...Int.max), .just(.max))

    /// Each op, simplest first.
    static let ops: [Gen<Op>] = [
        small.map { Op.map($0) },
        modulus.map { Op.filter($0) },
        modulus.map { Op.compactMap($0) },
        modulus.map { Op.flatMap($0) },
        .just(.tap),
        count.map { Op.take($0) },
        count.map { Op.drop($0) },
        small.map { Op.mapError($0) },
        .just(.tapError),
        value.map { Op.alt($0) },
        modulus.map { Op.flatMapError($0) },
        small.map { Op.getOrElse($0) },
        .just(.fold),
        modulus.map { Op.match($0) },
        value.map { Op.matchValue($0) },
        .just(.matchType),
        modulus.map { Op.matchError($0) },
        value.map { Op.matchErrorValue($0) },
        .just(.matchErrorType),
        value.map { Op.tapMatch($0) },
        .just(.tapMatchType),
        modulus.map { Op.tapMatchError($0) },
        bufferSize.map { Op.flatMapSequence(bufferSize: $0) },
        bufferSize.map { Op.flatMapAsyncSequence(bufferSize: $0) },
    ]

    static let op: Gen<Op> = .oneOf(ops)

    /// A few ops, by index into `ops`.
    static let opMenu: Gen<[Int]> = .array(of: .int(in: 0..<ops.count), count: 1...4)

    /// Every op first, so a failing pipe shrinks toward the plain mix; or a
    /// few, so some pipes are all failure handling, or all concurrent maps.
    static let policy: Gen<PipePolicy> = Hegel.zip(
        failureRate,
        Gen<[Int]?>.oneOf(.just(nil), opMenu.map { Optional($0) }),
        .element(of: [0.5, 0.0, 1.0]),
        .bool(probability: 0.25)
    ).map { PipePolicy(failureRate: $0, ops: $1, asyncRate: $2, tight: $3) }

    static func stage(
        _ op: Gen<Op>,
        asyncRate: Double,
        concurrency: Gen<Concurrency> = concurrency
    ) -> Gen<Stage> {
        Hegel.zip(op, .bool(probability: asyncRate), concurrency, .int()).map {
            op,
            isAsync,
            concurrency,
            salt in
            guard op.hasAsync, isAsync else { return Stage(op: op, async: nil, salt: salt) }
            return Stage(op: op, async: op.hasConcurrency ? concurrency : .sequential, salt: salt)
        }
    }

    /// Each expanding stage can double the elements. Past a budget of
    /// `1 << 16` possible outputs, a run would take too long for the
    /// deadline, so the later expanding stages become taps.
    static func capExpansion(_ stages: [Stage], inputCount: Int) -> [Stage] {
        var stages = stages
        var most = max(inputCount, 1)
        for index in stages.indices where stages[index].op.expands {
            if most > (1 << 16) / 2 {
                stages[index] = Stage(op: .tap, async: nil, salt: 0)
            } else {
                most *= 2
            }
        }
        return stages
    }

    /// Stages whose output does not depend on timing: an unordered stage is
    /// never followed by `Take` or `Drop`, since which elements those keep
    /// would depend on which finish first.
    static func orderedBeforeCounts(_ stages: [Stage]) -> [Stage] {
        var stages = stages
        var countsLater = false
        for index in stages.indices.reversed() {
            if stages[index].op.isCount {
                countsLater = true
            } else if countsLater, stages[index].isUnordered {
                stages[index].async?.ordered = true
            }
        }
        return stages
    }

    /// Stages the policy allows, a few usually and sometimes many.
    static func stages(_ policy: PipePolicy) -> Gen<[Stage]> {
        let op = policy.ops.map { menu in Gen.oneOf(menu.map { ops[$0] }) } ?? op
        let concurrency = policy.tight ? Gen.oneOf(.just(.sequential), tight) : concurrency
        let stage = stage(op, asyncRate: policy.asyncRate, concurrency: concurrency)
        return Gen.oneOf(.array(of: stage), .array(of: stage, count: 9...Int.max))
            .map(orderedBeforeCounts)
    }

    static let source: Gen<SourceKind> = .element(of: SourceKind.allCases)
    /// Suspensions before each read. Each costs time on every read, and more
    /// when other tests share the pool, so the long branch stops at 30: still
    /// several times what a stage's jitter yields.
    static let sourceYields: Gen<Int> = .oneOf(.just(0), .int(in: 0...3), .int(in: 0...30))

    static func spec(_ policy: PipePolicy) -> Gen<PipeSpec> {
        Hegel.zip(input(failureRate: policy.failureRate), stages(policy), source, sourceYields).map
        {
            PipeSpec(
                input: $0,
                stages: capExpansion($1, inputCount: $0.count),
                source: $2,
                sourceYields: $3
            )
        }
    }
}

#endif

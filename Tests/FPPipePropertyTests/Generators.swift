// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import HegelTesting

enum PipeGen {
    /// Small values usually, so patterns match often; any `Int` sometimes.
    static let value: Gen<Int> = .oneOf(.int(in: 0...30), .int(), .element(of: [.max, .min, -1]))

    static let element: Gen<Element> = .oneOf(
        value.map { Element.success($0) },
        value.map { Element.failure(Fail(code: $0)) }
    )

    /// A few elements usually; sometimes many. Hegel keeps arrays short
    /// unless told otherwise, so the long ones get a minimum length.
    static let input: Gen<[Element]> = .oneOf(
        .array(of: element),
        .array(of: element, count: 50...Int.max)
    )

    /// Concurrent: more than one at once, in either order.
    static let parallel: Gen<Concurrency> = Hegel.zip(
        .oneOf(.int(in: 2...6), .int(in: 2...Int.max), .just(.max)),
        .bool(),
        .oneOf(.just(Int.max), .int(in: 0...4), .int(in: 0...Int.max), .int(in: 1_020...1_030))
    ).map {
        Concurrency(concurrency: $0, ordered: $1, bufferLimit: $2)
    }

    static let concurrency: Gen<Concurrency> = .oneOf(.just(.sequential), parallel)

    /// Small moduli match often; any `Int` is valid, 0 and negatives too.
    static let modulus: Gen<Int> = .oneOf(.int(in: -4...4), .int(), .element(of: [.min, .max]))
    static let small: Gen<Int> = .oneOf(.int(in: 0...5), .int())
    /// `Take` and `Drop` need at least 0.
    static let count: Gen<Int> = .oneOf(.int(in: 0...6), .int(in: 0...Int.max), .just(.max))
    /// At least 1.
    static let bufferSize: Gen<Int> = .oneOf(.int(in: 1...4), .int(in: 1...Int.max), .just(.max))

    static let op: Gen<Op> = .oneOf(
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
        bufferSize.map { Op.flatMapAsyncSequence(bufferSize: $0) }
    )

    static let stage: Gen<Stage> = Hegel.zip(op, .bool(), concurrency, .int()).map {
        op,
        isAsync,
        concurrency,
        salt in
        guard op.hasAsync, isAsync else { return Stage(op: op, async: nil, salt: salt) }
        return Stage(op: op, async: op.hasConcurrency ? concurrency : .sequential, salt: salt)
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

    /// Any stages, a few usually and sometimes many.
    static let anyStages: Gen<[Stage]> = Gen.oneOf(
        .array(of: stage),
        .array(of: stage, count: 9...Int.max)
    )

    static let stages: Gen<[Stage]> = anyStages.map(orderedBeforeCounts)

    static let source: Gen<SourceKind> = .element(of: SourceKind.allCases)
    /// Suspensions before each read. Each costs time on every read, and more
    /// when other tests share the pool, so the long branch stops at 30: still
    /// several times what a stage's jitter yields.
    static let sourceYields: Gen<Int> = .oneOf(.just(0), .int(in: 0...3), .int(in: 0...30))

    static let spec: Gen<PipeSpec> = Hegel.zip(input, stages, source, sourceYields).map {
        PipeSpec(
            input: $0,
            stages: capExpansion($1, inputCount: $0.count),
            source: $2,
            sourceYields: $3
        )
    }
}

#endif

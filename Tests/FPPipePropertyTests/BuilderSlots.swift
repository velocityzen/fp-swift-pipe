// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

/// What the builder bodies use, drawn by the property.
struct BuilderParameters: Sendable {
    var on: [Bool]
    var branch: Int
    var a: Int
    var m: Int
    var x: Int
    var n: Int
    var size: Int
    var c: Int
    var b: Int
}

enum BuilderMode: Sendable, Hashable, CaseIterable {
    case pipe, openPipe, widened
}

/// The stages the builder bodies in BuilderBodies.swift switch on, as the
/// model sees them, with the index each one logs under.
enum BuilderSlots {
    /// The optional stages; the `if`/`else` and `switch` come after them.
    static let count = 25

    static func stages(_ p: BuilderParameters) -> [(index: Int, stage: Stage)] {
        let async = Concurrency(concurrency: p.c, ordered: true, bufferLimit: p.b)
        let optional: [Stage] = [
            Stage(op: .map(p.a), async: nil, salt: 0),
            Stage(op: .filter(p.m), async: nil, salt: 0),
            Stage(op: .compactMap(p.m), async: nil, salt: 0),
            Stage(op: .flatMap(p.m), async: nil, salt: 0),
            Stage(op: .tap, async: nil, salt: 0),
            Stage(op: .mapError(p.a), async: nil, salt: 0),
            Stage(op: .map(p.a), async: async, salt: 0),
            Stage(op: .match(p.m), async: nil, salt: 0),
            Stage(op: .matchValue(p.x), async: nil, salt: 0),
            Stage(op: .matchType, async: nil, salt: 0),
            Stage(op: .flatMapError(p.m), async: nil, salt: 0),
            Stage(op: .matchError(p.m), async: nil, salt: 0),
            Stage(op: .matchErrorValue(p.x), async: nil, salt: 0),
            Stage(op: .matchErrorType, async: nil, salt: 0),
            Stage(op: .alt(p.x), async: nil, salt: 0),
            Stage(op: .take(p.n), async: nil, salt: 0),
            Stage(op: .drop(p.n / 2), async: nil, salt: 0),
            Stage(op: .flatMapSequence(bufferSize: p.size), async: nil, salt: 0),
            Stage(op: .flatMapAsyncSequence(bufferSize: p.size), async: nil, salt: 0),
            Stage(op: .tapError, async: nil, salt: 0),
            Stage(op: .tapMatch(p.x), async: nil, salt: 0),
            Stage(op: .tapMatchType, async: nil, salt: 0),
            Stage(op: .tapMatchError(p.m), async: nil, salt: 0),
            Stage(op: .filter(p.m), async: async, salt: 1),
        ]
        precondition(optional.count + 1 == count)
        var stages = optional.indices.filter { p.on[$0] }.map { ($0, optional[$0]) }
        let either = optional.count
        stages.append(
            (either, Stage(op: .map(p.on[either] ? p.a : p.a &+ 1), async: nil, salt: 0))
        )
        let branch: Stage =
            switch p.branch {
                case 0: Stage(op: .flatMapError(p.m), async: .sequential, salt: 2)
                case 1: Stage(op: .flatMapError(p.m), async: async, salt: 2)
                default: Stage(op: .flatMapError(2), async: async, salt: 2)
            }
        stages.append((either + 1, branch))
        return stages
    }
}

#endif

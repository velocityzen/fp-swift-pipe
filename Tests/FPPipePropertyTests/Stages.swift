// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import Synchronization

// Random pipelines of `Int` values and `Fail` failures, built two ways: for
// real, from FPPipe's stages (RealStages.swift), and as a model, from plain
// pull-based iterators that apply the same per-element functions
// (Model.swift). The properties check that the plumbing (laziness, ordering,
// concurrency, early stops) agrees.

struct Fail: Error, Hashable, Sendable {
    let code: Int
}

typealias Element = Result<Int, Fail>

/// What the pipe did, in order: each source read and each side effect. It
/// also counts each stage's async closures: how many ran, and how many at
/// once.
final class Log: Sendable {
    private let entries = Mutex<[String]>([])
    /// Per stage index: the closures running now, the most at once, and how
    /// many ran.
    private let running = Mutex<[Int: (now: Int, peak: Int, runs: Int)]>([:])
    /// Called with each entry after it is added.
    private let onEntry: @Sendable (String) -> Void
    /// Called on each entry and as each async closure starts and ends: the
    /// pipe is making progress.
    private let onProgress: @Sendable () -> Void

    init(
        onEntry: @escaping @Sendable (String) -> Void = { _ in },
        onProgress: @escaping @Sendable () -> Void = {}
    ) {
        self.onEntry = onEntry
        self.onProgress = onProgress
    }

    func add(_ entry: String) {
        entries.withLock { $0.append(entry) }
        onEntry(entry)
        onProgress()
    }

    var all: [String] { entries.withLock { $0 } }

    /// How many elements the source gave.
    var reads: Int { entries.withLock { $0.count(where: { $0.hasPrefix("read ") }) } }

    /// Runs stage `stage`'s async closure body, counted as running for all of
    /// it.
    func running<T>(_ stage: Int, _ body: () async -> T) async -> T {
        running.withLock { counts in
            let old = counts[stage] ?? (0, 0, 0)
            counts[stage] = (old.now + 1, max(old.now + 1, old.peak), old.runs + 1)
        }
        onProgress()
        let value = await body()
        running.withLock { $0[stage]!.now -= 1 }
        onProgress()
        return value
    }

    /// The most of stage `stage`'s closures that ran at once, 0 if none ran.
    func peak(_ stage: Int) -> Int { running.withLock { $0[stage]?.peak ?? 0 } }

    /// How many of stage `stage`'s closures ran.
    func runs(_ stage: Int) -> Int { running.withLock { $0[stage]?.runs ?? 0 } }
}

// MARK: - Stages

/// How an async stage runs. Concurrency 1 is the sequential version.
struct Concurrency: Sendable, Hashable, CustomStringConvertible {
    var concurrency: Int
    var ordered: Bool
    var bufferLimit: Int

    static let sequential = Concurrency(concurrency: 1, ordered: false, bufferLimit: .max)

    var isConcurrent: Bool { concurrency > 1 }
    var isUnordered: Bool { concurrency > 1 && !ordered }

    /// The most closures the docs let run at once: `concurrency`, and with
    /// `ordered`, `bufferLimit + 1` when that is lower. The tap stages have
    /// no `concurrency:`, so theirs is `.sequential`: one at a time.
    var closureLimit: Int {
        guard ordered, isConcurrent else { return concurrency }
        return min(concurrency, bufferLimit == .max ? .max : bufferLimit + 1)
    }

    var description: String {
        guard isConcurrent else { return "" }
        let limit = bufferLimit == .max ? "" : ", bufferLimit: \(bufferLimit)"
        return "concurrency: \(concurrency), ordered: \(ordered)\(limit)"
    }
}

enum Op: Sendable, Hashable {
    case map(Int)
    case filter(Int)
    case compactMap(Int)
    case flatMap(Int)
    case tap
    case take(Int)
    case drop(Int)
    case mapError(Int)
    case tapError
    case alt(Int)
    case flatMapError(Int)
    case getOrElse(Int)
    case fold
    case match(Int)
    case matchValue(Int)
    case matchType
    case matchError(Int)
    case matchErrorValue(Int)
    case matchErrorType
    case tapMatch(Int)
    case tapMatchType
    case tapMatchError(Int)
    case flatMapSequence(bufferSize: Int)
    case flatMapAsyncSequence(bufferSize: Int)

    /// The op has an `Async` version.
    var hasAsync: Bool {
        switch self {
            case .take, .drop, .mapError, .flatMapSequence, .flatMapAsyncSequence: false
            default: true
        }
    }

    /// The async version takes `concurrency:`.
    var hasConcurrency: Bool {
        switch self {
            case .tap, .tapError, .tapMatch, .tapMatchType, .tapMatchError: false
            default: hasAsync
        }
    }

    var isTake: Bool {
        if case .take = self { true } else { false }
    }

    var isCount: Bool {
        switch self {
            case .take, .drop: true
            default: false
        }
    }

    /// The op can give nothing for an element.
    var drops: Bool {
        switch self {
            case .filter, .compactMap, .take, .drop, .flatMapSequence, .flatMapAsyncSequence: true
            default: false
        }
    }

    var expands: Bool {
        switch self {
            case .flatMapSequence, .flatMapAsyncSequence: true
            default: false
        }
    }

    /// What an op works on. The real stages and the model handle each family
    /// on its own, so no one switch covers every op.
    enum Family {
        /// Successes, or every element.
        case value
        /// Failures.
        case failure
        /// Successes that match a pattern.
        case match
        /// Failures that match a pattern.
        case matchError
    }

    var family: Family {
        switch self {
            case .map, .filter, .compactMap, .flatMap, .tap, .take, .drop, .flatMapSequence,
                .flatMapAsyncSequence:
                .value
            case .mapError, .tapError, .alt, .flatMapError, .getOrElse, .fold: .failure
            case .match, .matchValue, .matchType, .tapMatch, .tapMatchType: .match
            case .matchError, .matchErrorValue, .matchErrorType, .tapMatchError: .matchError
        }
    }
}

/// One stage of a generated pipe: the op, and how its async version runs,
/// or `nil` for the sync version.
struct Stage: Sendable, Hashable, CustomStringConvertible {
    var op: Op
    var async: Concurrency?
    /// Varies the async closure's timing.
    var salt: Int

    var isConcurrent: Bool {
        switch op {
            case .flatMapSequence(let size), .flatMapAsyncSequence(let size): size > 1
            default: async?.isConcurrent ?? false
        }
    }

    var isUnordered: Bool { async?.isUnordered ?? false }

    var description: String {
        let name: String
        let argument: String
        switch op {
            case .map(let a): (name, argument) = ("Map", "+\(a)")
            case .filter(let m): (name, argument) = ("Filter", "% \(m) != 0")
            case .compactMap(let m): (name, argument) = ("CompactMap", "% \(m) == 0 → nil, else +1")
            case .flatMap(let m): (name, argument) = ("FlatMap", "% \(m) == 0 → failure")
            case .tap: (name, argument) = ("Tap", "")
            case .take(let n): return "Take(\(n))"
            case .drop(let n): return "Drop(\(n))"
            case .mapError(let k): (name, argument) = ("MapError", "+\(k)")
            case .tapError: (name, argument) = ("TapError", "")
            case .alt(let x): (name, argument) = ("Alt", "\(x)")
            case .flatMapError(let m): (name, argument) = ("FlatMapError", "% \(m) == 0 → success")
            case .getOrElse(let k): (name, argument) = ("GetOrElse", "code + \(k)")
            case .fold: (name, argument) = ("Match", "onSuccess: × 3, onFailure: -code")
            case .match(let m): (name, argument) = ("Match", "where % \(m) == 0")
            case .matchValue(let x): (name, argument) = ("Match", "\(x)")
            case .matchType: (name, argument) = ("Match", "Int.self")
            case .matchError(let m): (name, argument) = ("MatchError", "extract % \(m) == 0")
            case .matchErrorValue(let c): (name, argument) = ("MatchError", "Fail(code: \(c))")
            case .matchErrorType: (name, argument) = ("MatchError", "Fail.self")
            case .tapMatch(let x): (name, argument) = ("TapMatch", "\(x)")
            case .tapMatchType: (name, argument) = ("TapMatch", "Int.self")
            case .tapMatchError(let m): (name, argument) = ("TapMatchError", "where % \(m) == 0")
            case .flatMapSequence(let size): return "FlatMapSequence(bufferSize: \(size))"
            case .flatMapAsyncSequence(let size): return "FlatMapAsyncSequence(bufferSize: \(size))"
        }
        guard let async else { return "\(name) { \(argument) }" }
        return "Async\(name)(\(async)) { \(argument) }"
    }
}

// MARK: - The per-element functions, shared by both sides

/// Wrapping arithmetic throughout: the values can be any `Int`.
enum F {
    static func map(_ a: Int, _ v: Int) -> Int { v &+ a }
    static func filter(_ m: Int, _ v: Int) -> Bool { !v.isMultiple(of: m) }
    static func compactMap(_ m: Int, _ v: Int) -> Int? { v.isMultiple(of: m) ? nil : v &+ 1 }
    static func flatMap(_ m: Int, _ v: Int) -> Element {
        v.isMultiple(of: m) ? .failure(Fail(code: v)) : .success(v)
    }
    static func mapError(_ k: Int, _ f: Fail) -> Fail { Fail(code: f.code &+ k) }
    static func alt(_ x: Int) -> Element { x % 2 == 0 ? .success(x) : .failure(Fail(code: x)) }
    static func flatMapError(_ m: Int, _ f: Fail) -> Element {
        f.code.isMultiple(of: m) ? .success(f.code) : .failure(Fail(code: f.code &+ 1))
    }
    static func recover(_ k: Int, _ f: Fail) -> Int { f.code &+ k }
    static func foldSuccess(_ v: Int) -> Int { v &* 3 }
    static func foldFailure(_ f: Fail) -> Int { 0 &- f.code }
    static func matches(_ m: Int, _ v: Int) -> Bool { v.isMultiple(of: m) }
    static func matched(_ v: Int) -> Element {
        v % 2 == 0 ? .success(v &+ 100) : .failure(Fail(code: v))
    }
    static func matchedValue(_ x: Int) -> Element {
        x % 2 == 0 ? .success(x &+ 1) : .failure(Fail(code: x))
    }
    static func extract(_ m: Int, _ f: Fail) -> Int? { f.code.isMultiple(of: m) ? f.code : nil }
    static func extracted(_ payload: Int) -> Element { .success(payload &* 2) }
    static func errorMatches(_ m: Int, _ f: Fail) -> Bool { f.code.isMultiple(of: m) }
    /// 0, 1 or 2 copies of `v`, for negative values too.
    static func expand(_ v: Int) -> [Int] { Array(repeating: v, count: (v % 3 + 3) % 3) }
}

/// Suspends a few times, more for some values than others, so concurrent
/// work finishes out of order.
func jitter(_ value: Int, _ salt: Int) async {
    let turns = (value &* 31 &+ salt &* 7) & 7
    for _ in 0..<turns {
        await Task.yield()
    }
    if turns > 5 {
        // Without a tolerance the system may coalesce the timer: a 60µs
        // sleep then takes about a millisecond.
        try? await Task.sleep(for: .microseconds(turns * 10), tolerance: .zero)
    }
}

#endif

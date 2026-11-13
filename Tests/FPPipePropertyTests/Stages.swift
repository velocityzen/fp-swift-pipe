import FPPipe
import HegelTesting
import Synchronization

// Random pipelines of `Int` values and `Fail` failures, built two ways: for
// real, from FPPipe's stages, and as a model, from plain pull-based iterators
// that apply the same per-element functions. The properties check that the
// plumbing (laziness, ordering, concurrency, early stops) agrees.

struct Fail: Error, Hashable, Sendable {
    let code: Int
}

typealias Element = Result<Int, Fail>

/// What the pipe did, in order: each source read and each side effect.
final class Log: Sendable {
    private let entries = Mutex<[String]>([])
    /// Called with each entry after it is added.
    private let onEntry: @Sendable (String) -> Void

    init(onEntry: @escaping @Sendable (String) -> Void = { _ in }) {
        self.onEntry = onEntry
    }

    func add(_ entry: String) {
        entries.withLock { $0.append(entry) }
        onEntry(entry)
    }

    var all: [String] { entries.withLock { $0 } }
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

    var expands: Bool {
        switch self {
            case .flatMapSequence, .flatMapAsyncSequence: true
            default: false
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
        try? await Task.sleep(for: .microseconds(turns * 10))
    }
}

// MARK: - Sources

/// The input, logging each read. Each read can suspend first, like a slow
/// source.
struct LoggedSource: AsyncSequence, Sendable {
    typealias Failure = Never
    let elements: [Element]
    let log: Log
    var yields = 0

    struct AsyncIterator: AsyncIteratorProtocol {
        let elements: [Element]
        let log: Log
        let yields: Int
        var index = 0

        mutating func next() async -> Element? {
            for _ in 0..<yields {
                await Task.yield()
            }
            guard index < elements.count else { return nil }
            log.add("read \(index)")
            defer { index += 1 }
            return elements[index]
        }
    }

    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(elements: elements, log: log, yields: yields)
    }
}

/// How the pipe gets its input.
enum SourceKind: Sendable, Hashable, CaseIterable {
    case fromResult, deferResult, fromAsyncResult, openPipe
}

/// An async sequence of `count` copies of `value`, suspending before each.
struct Repeated: AsyncSequence, Sendable {
    typealias Failure = Never
    let value: Int
    let count: Int

    struct AsyncIterator: AsyncIteratorProtocol {
        let value: Int
        var remaining: Int

        mutating func next() async -> Int? {
            await Task.yield()
            guard remaining > 0 else { return nil }
            remaining -= 1
            return value
        }
    }

    func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(value: value, remaining: count)
    }
}

// MARK: - The real pipe

/// Gives a pipe that cannot fail the `Fail` failure type again, through the
/// builder's widening, so the chain goes on.
func widened(_ pipe: Pipe<Int, Never>) -> Pipe<Int, Fail> {
    Pipe<Int, Fail> {
        FromResult(pipe)
        FlatMap { (v: Int) -> Element in .success(v) }
    }
}

extension Stage {
    /// Attaches the real stage, number `index` in the pipe, to `upstream`.
    func attach(_ upstream: Pipe<Int, Fail>, index i: Int, log: Log) -> Pipe<Int, Fail> {
        let salt = self.salt
        guard let c = async else {
            switch op {
                case .map(let a): return Map { (v: Int) in F.map(a, v) }.attach(upstream)
                case .filter(let m): return Filter { (v: Int) in F.filter(m, v) }.attach(upstream)
                case .compactMap(let m):
                    return CompactMap { (v: Int) in F.compactMap(m, v) }.attach(upstream)
                case .flatMap(let m):
                    return FlatMap { (v: Int) in F.flatMap(m, v) }.attach(upstream)
                case .tap: return Tap { (v: Int) in log.add("\(i) tap \(v)") }.attach(upstream)
                case .take(let n): return Take(n).attach(upstream)
                case .drop(let n): return Drop(n).attach(upstream)
                case .mapError(let k):
                    return MapError { (f: Fail) in F.mapError(k, f) }.attach(upstream)
                case .tapError:
                    return TapError { (f: Fail) in log.add("\(i) tapError \(f.code)") }
                        .attach(upstream)
                case .alt(let x): return Alt { F.alt(x) }.attach(upstream)
                case .flatMapError(let m):
                    return FlatMapError { (f: Fail) in F.flatMapError(m, f) }.attach(upstream)
                case .getOrElse(let k):
                    return widened(GetOrElse { (f: Fail) in F.recover(k, f) }.attach(upstream))
                case .fold:
                    return widened(
                        Match(onSuccess: F.foldSuccess, onFailure: F.foldFailure).attach(upstream)
                    )
                case .match(let m):
                    return Match(where: { (v: Int) in F.matches(m, v) }, then: F.matched)
                        .attach(upstream)
                case .matchValue(let x):
                    return Match(x, then: { F.matchedValue(x) }).attach(upstream)
                case .matchType:
                    return Match(Int.self, then: { (v: Int) -> Element in F.matched(v) })
                        .attach(upstream)
                case .matchError(let m):
                    return MatchError(extract: { (f: Fail) in F.extract(m, f) }, then: F.extracted)
                        .attach(upstream)
                case .matchErrorValue(let code):
                    return MatchError(Fail(code: code), then: { F.extracted(code) })
                        .attach(upstream)
                case .matchErrorType:
                    return MatchError(
                        Fail.self,
                        then: { (f: Fail) -> Element in F.flatMapError(2, f) }
                    )
                    .attach(upstream)
                case .tapMatch(let x):
                    return TapMatch(x, then: { log.add("\(i) tapMatch") }).attach(upstream)
                case .tapMatchType:
                    return TapMatch(
                        Int.self,
                        then: { (v: Int) in log.add("\(i) tapMatchType \(v)") }
                    )
                    .attach(upstream)
                case .tapMatchError(let m):
                    return TapMatchError(
                        where: { (f: Fail) in F.errorMatches(m, f) },
                        then: { (f: Fail) in log.add("\(i) tapMatchError \(f.code)") }
                    ).attach(upstream)
                case .flatMapSequence(let size):
                    return FlatMapSequence(bufferSize: size) { (v: Int) in F.expand(v) }
                        .attach(upstream)
                case .flatMapAsyncSequence(let size):
                    return FlatMapAsyncSequence(bufferSize: size) { (v: Int) in
                        Repeated(value: v, count: F.expand(v).count)
                    }.attach(upstream)
            }
        }
        let (n, o, b) = (c.concurrency, c.ordered, c.bufferLimit)
        switch op {
            case .map(let a):
                return AsyncMap(concurrency: n, ordered: o, bufferLimit: b) { (v: Int) in
                    await jitter(v, salt)
                    return F.map(a, v)
                }.attach(upstream)
            case .filter(let m):
                return AsyncFilter(concurrency: n, ordered: o, bufferLimit: b) { (v: Int) in
                    await jitter(v, salt)
                    return F.filter(m, v)
                }.attach(upstream)
            case .compactMap(let m):
                return AsyncCompactMap(concurrency: n, ordered: o, bufferLimit: b) { (v: Int) in
                    await jitter(v, salt)
                    return F.compactMap(m, v)
                }.attach(upstream)
            case .flatMap(let m):
                return AsyncFlatMap(concurrency: n, ordered: o, bufferLimit: b) { (v: Int) in
                    await jitter(v, salt)
                    return F.flatMap(m, v)
                }.attach(upstream)
            case .tap:
                return AsyncTap { (v: Int) in
                    await jitter(v, salt)
                    log.add("\(i) tap \(v)")
                }.attach(upstream)
            case .tapError:
                return AsyncTapError { (f: Fail) in
                    await jitter(f.code, salt)
                    log.add("\(i) tapError \(f.code)")
                }.attach(upstream)
            case .alt(let x):
                return AsyncAlt(concurrency: n, ordered: o, bufferLimit: b) {
                    await jitter(x, salt)
                    return F.alt(x)
                }.attach(upstream)
            case .flatMapError(let m):
                return AsyncFlatMapError(concurrency: n, ordered: o, bufferLimit: b) { (f: Fail) in
                    await jitter(f.code, salt)
                    return F.flatMapError(m, f)
                }.attach(upstream)
            case .getOrElse(let k):
                return widened(
                    AsyncGetOrElse(concurrency: n, ordered: o, bufferLimit: b) { (f: Fail) in
                        await jitter(f.code, salt)
                        return F.recover(k, f)
                    }.attach(upstream)
                )
            case .fold:
                return widened(
                    AsyncMatch(
                        concurrency: n,
                        ordered: o,
                        bufferLimit: b,
                        onSuccess: { (v: Int) in
                            await jitter(v, salt)
                            return F.foldSuccess(v)
                        },
                        onFailure: { (f: Fail) in
                            await jitter(f.code, salt)
                            return F.foldFailure(f)
                        }
                    ).attach(upstream)
                )
            case .match(let m):
                return AsyncMatch(
                    where: { (v: Int) in F.matches(m, v) },
                    concurrency: n,
                    ordered: o,
                    bufferLimit: b,
                    then: { (v: Int) in
                        await jitter(v, salt)
                        return F.matched(v)
                    }
                ).attach(upstream)
            case .matchValue(let x):
                return AsyncMatch(
                    x,
                    concurrency: n,
                    ordered: o,
                    bufferLimit: b,
                    then: {
                        await jitter(x, salt)
                        return F.matchedValue(x)
                    }
                ).attach(upstream)
            case .matchType:
                return AsyncMatch(
                    Int.self,
                    concurrency: n,
                    ordered: o,
                    bufferLimit: b,
                    then: { (v: Int) -> Element in
                        await jitter(v, salt)
                        return F.matched(v)
                    }
                ).attach(upstream)
            case .matchError(let m):
                return AsyncMatchError(
                    extract: { (f: Fail) in F.extract(m, f) },
                    concurrency: n,
                    ordered: o,
                    bufferLimit: b,
                    then: { (payload: Int) in
                        await jitter(payload, salt)
                        return F.extracted(payload)
                    }
                ).attach(upstream)
            case .matchErrorValue(let code):
                return AsyncMatchError(
                    Fail(code: code),
                    concurrency: n,
                    ordered: o,
                    bufferLimit: b,
                    then: {
                        await jitter(code, salt)
                        return F.extracted(code)
                    }
                ).attach(upstream)
            case .matchErrorType:
                return AsyncMatchError(
                    Fail.self,
                    concurrency: n,
                    ordered: o,
                    bufferLimit: b,
                    then: { (f: Fail) -> Element in
                        await jitter(f.code, salt)
                        return F.flatMapError(2, f)
                    }
                ).attach(upstream)
            case .tapMatch(let x):
                return AsyncTapMatch(x) {
                    await jitter(x, salt)
                    log.add("\(i) tapMatch")
                }.attach(upstream)
            case .tapMatchType:
                return AsyncTapMatch(Int.self) { (v: Int) in
                    await jitter(v, salt)
                    log.add("\(i) tapMatchType \(v)")
                }.attach(upstream)
            case .tapMatchError(let m):
                return AsyncTapMatchError(
                    where: { (f: Fail) in F.errorMatches(m, f) },
                    then: { (f: Fail) in
                        await jitter(f.code, salt)
                        log.add("\(i) tapMatchError \(f.code)")
                    }
                ).attach(upstream)
            case .take, .drop, .mapError, .flatMapSequence, .flatMapAsyncSequence:
                preconditionFailure("\(op) has no async version")
        }
    }
}

/// The input as a pipe, read the way `kind` says.
func sourcePipe(_ source: LoggedSource, _ kind: SourceKind) -> Pipe<Int, Fail> {
    switch kind {
        case .fromResult:
            return Pipe<Int, Fail> { FromResult(source) }
        case .deferResult:
            return Pipe<Int, Fail> { DeferResult { source } }
        case .fromAsyncResult:
            return Pipe<Int, Fail> {
                FromAsyncResult {
                    await Task.yield()
                    return source
                }
            }
        case .openPipe:
            let open = OpenPipe {
                FromResult(Int.self, Fail.self)
                Tap { (_: Int) in }
            }
            return open(source)
    }
}

/// A generated pipe: its input, how it is read, and its stages in order.
struct PipeSpec: Sendable, CustomStringConvertible {
    var input: [Element]
    var stages: [Stage]
    var source: SourceKind = .fromResult
    var sourceYields = 0

    func pipe(log: Log) -> Pipe<Int, Fail> {
        var pipe = sourcePipe(LoggedSource(elements: input, log: log, yields: sourceYields), source)
        for (index, stage) in stages.enumerated() {
            pipe = stage.attach(pipe, index: index, log: log)
        }
        return pipe
    }

    var description: String { "\(source), yields \(sourceYields) → \(stages)" }
}

func realPipe(_ input: [Element], _ stages: [Stage], log: Log) -> Pipe<Int, Fail> {
    PipeSpec(input: input, stages: stages).pipe(log: log)
}

// MARK: - The model

/// A model pipe: a function that gives the next element, or `nil` at the
/// end. It pulls from upstream only when asked, like the real one when it
/// runs sequentially.
typealias Pull = () -> Element?

final class ModelLog {
    var entries: [String] = []
}

func modelPipe(_ input: [Element], _ stages: [Stage], log: ModelLog) -> Pull {
    var index = 0
    var pull: Pull = {
        guard index < input.count else { return nil }
        log.entries.append("read \(index)")
        defer { index += 1 }
        return input[index]
    }
    for (i, stage) in stages.enumerated() {
        pull = stage.op.model(pull, index: i, log: log)
    }
    return pull
}

extension Op {
    func model(_ up: @escaping Pull, index i: Int, log: ModelLog) -> Pull {
        /// Pulls until `step` keeps an element.
        func each(_ step: @escaping (Element) -> Element?) -> Pull {
            {
                while let element = up() {
                    if let output = step(element) { return output }
                }
                return nil
            }
        }
        func onSuccess(_ step: @escaping (Int) -> Element?) -> Pull {
            each { element in
                guard case .success(let v) = element else { return element }
                return step(v)
            }
        }
        func onFailure(_ step: @escaping (Fail) -> Element) -> Pull {
            each { element in
                guard case .failure(let f) = element else { return element }
                return step(f)
            }
        }
        switch self {
            case .map(let a): return onSuccess { .success(F.map(a, $0)) }
            case .filter(let m): return onSuccess { F.filter(m, $0) ? .success($0) : nil }
            case .compactMap(let m): return onSuccess { F.compactMap(m, $0).map(Element.success) }
            case .flatMap(let m): return onSuccess { F.flatMap(m, $0) }
            case .tap:
                return onSuccess { v in
                    log.entries.append("\(i) tap \(v)")
                    return .success(v)
                }
            case .take(let n):
                var remaining = n
                return {
                    guard remaining > 0 else { return nil }
                    remaining -= 1
                    return up()
                }
            case .drop(let n):
                var remaining = n
                return {
                    while remaining > 0 {
                        remaining -= 1
                        guard up() != nil else { return nil }
                    }
                    return up()
                }
            case .mapError(let k): return onFailure { .failure(F.mapError(k, $0)) }
            case .tapError:
                return onFailure { f in
                    log.entries.append("\(i) tapError \(f.code)")
                    return .failure(f)
                }
            case .alt(let x): return onFailure { _ in F.alt(x) }
            case .flatMapError(let m): return onFailure { F.flatMapError(m, $0) }
            case .getOrElse(let k): return onFailure { .success(F.recover(k, $0)) }
            case .fold:
                return each { element in
                    switch element {
                        case .success(let v): .success(F.foldSuccess(v))
                        case .failure(let f): .success(F.foldFailure(f))
                    }
                }
            case .match(let m): return onSuccess { F.matches(m, $0) ? F.matched($0) : .success($0) }
            case .matchValue(let x): return onSuccess { $0 == x ? F.matchedValue(x) : .success($0) }
            case .matchType: return onSuccess { F.matched($0) }
            case .matchError(let m):
                return onFailure { f in F.extract(m, f).map(F.extracted) ?? .failure(f) }
            case .matchErrorValue(let code):
                return onFailure { $0.code == code ? F.extracted(code) : .failure($0) }
            case .matchErrorType: return onFailure { F.flatMapError(2, $0) }
            case .tapMatch(let x):
                return onSuccess { v in
                    if v == x { log.entries.append("\(i) tapMatch") }
                    return .success(v)
                }
            case .tapMatchType:
                return onSuccess { v in
                    log.entries.append("\(i) tapMatchType \(v)")
                    return .success(v)
                }
            case .tapMatchError(let m):
                return onFailure { f in
                    if F.errorMatches(m, f) { log.entries.append("\(i) tapMatchError \(f.code)") }
                    return .failure(f)
                }
            case .flatMapSequence, .flatMapAsyncSequence:
                var pending: [Int] = []
                return {
                    while true {
                        if !pending.isEmpty { return .success(pending.removeFirst()) }
                        switch up() {
                            case nil: return nil
                            case .failure(let f)?: return .failure(f)
                            case .success(let v)?: pending = F.expand(v)
                        }
                    }
                }
        }
    }
}

// MARK: - Generators

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
    /// Suspensions before each read. Each costs time on every read, so the
    /// long branch stops at 1,000.
    static let sourceYields: Gen<Int> = .oneOf(.just(0), .int(in: 0...3), .int(in: 0...1_000))

    static let spec: Gen<PipeSpec> = Hegel.zip(input, stages, source, sourceYields).map {
        PipeSpec(
            input: $0,
            stages: capExpansion($1, inputCount: $0.count),
            source: $2,
            sourceYields: $3
        )
    }
}

// MARK: - Multisets

func counts<T: Hashable>(_ items: [T]) -> [T: Int] {
    items.reduce(into: [:]) { $0[$1, default: 0] += 1 }
}

/// Whether every item of `part` is in `whole`, as many times or fewer.
func isSubmultiset<T: Hashable>(_ part: [T], of whole: [T]) -> Bool {
    let available = counts(whole)
    return counts(part).allSatisfy { item, count in count <= available[item, default: 0] }
}

// MARK: - Deadlines

/// Resumes a continuation at most once.
final class Once<T: Sendable>: Sendable {
    let continuation: Mutex<CheckedContinuation<T?, Never>?>
    init(_ continuation: CheckedContinuation<T?, Never>) {
        self.continuation = Mutex(continuation)
    }
    func resume(_ value: T?) -> Bool {
        guard let continuation = continuation.withLock({ $0.take() }) else { return false }
        continuation.resume(returning: value)
        return true
    }
}

/// How long a pipe gets before the run counts as hung. Generous: the long
/// inputs through many concurrent stages are slow under a loaded machine.
let deadline = Duration.seconds(10)

/// Runs `body` in its own task, and gives up on it after `limit`: `nil`
/// means it was still running. A hung pipe then fails one test case instead
/// of hanging the whole run.
func withinDeadline<T: Sendable>(
    _ limit: Duration,
    _ body: @escaping @Sendable () async -> T
) async -> T? {
    await withCheckedContinuation { continuation in
        let once = Once(continuation)
        let work = Task {
            _ = once.resume(await body())
        }
        Task {
            try? await Task.sleep(for: limit)
            if once.resume(nil) {
                work.cancel()
            }
        }
    }
}

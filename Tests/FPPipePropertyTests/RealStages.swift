// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe

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
        let real = RealStage(upstream: upstream, i: i, log: log, salt: salt)
        switch (op.family, async) {
            case (.value, nil): return real.value(op)
            case (.value, let c?): return real.asyncValue(op, c)
            case (.failure, nil): return real.failure(op)
            case (.failure, let c?): return real.asyncFailure(op, c)
            case (.match, nil): return real.match(op)
            case (.match, let c?): return real.asyncMatch(op, c)
            case (.matchError, nil): return real.matchError(op)
            case (.matchError, let c?): return real.asyncMatchError(op, c)
        }
    }
}

/// What a real stage is attached with: the pipe before it, its index and
/// the log for its side effects, and the salt for its async closure's timing.
struct RealStage: Sendable {
    let upstream: Pipe<Int, Fail>
    let i: Int
    let log: Log
    let salt: Int
}

// MARK: - Values

extension RealStage {
    func value(_ op: Op) -> Pipe<Int, Fail> {
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
            case .flatMapSequence(let size):
                return FlatMapSequence(bufferSize: size) { (v: Int) in F.expand(v) }
                    .attach(upstream)
            case .flatMapAsyncSequence(let size):
                return FlatMapAsyncSequence(bufferSize: size) { (v: Int) in
                    Repeated(value: v, count: F.expand(v).count)
                }.attach(upstream)
            default: preconditionFailure("\(op) is not a value op")
        }
    }

    func asyncValue(_ op: Op, _ c: Concurrency) -> Pipe<Int, Fail> {
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
            default: preconditionFailure("\(op) has no async version, or is not a value op")
        }
    }
}

// MARK: - Failures

extension RealStage {
    func failure(_ op: Op) -> Pipe<Int, Fail> {
        switch op {
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
            default: preconditionFailure("\(op) is not a failure op")
        }
    }

    func asyncFailure(_ op: Op, _ c: Concurrency) -> Pipe<Int, Fail> {
        let (n, o, b) = (c.concurrency, c.ordered, c.bufferLimit)
        switch op {
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
            default: preconditionFailure("\(op) has no async version, or is not a failure op")
        }
    }
}

// MARK: - Matching values

extension RealStage {
    func match(_ op: Op) -> Pipe<Int, Fail> {
        switch op {
            case .match(let m):
                return Match(where: { (v: Int) in F.matches(m, v) }, then: F.matched)
                    .attach(upstream)
            case .matchValue(let x):
                return Match(x, then: { F.matchedValue(x) }).attach(upstream)
            case .matchType:
                return Match(Int.self, then: { (v: Int) -> Element in F.matched(v) })
                    .attach(upstream)
            case .tapMatch(let x):
                return TapMatch(x, then: { log.add("\(i) tapMatch") }).attach(upstream)
            case .tapMatchType:
                return TapMatch(
                    Int.self,
                    then: { (v: Int) in log.add("\(i) tapMatchType \(v)") }
                )
                .attach(upstream)
            default: preconditionFailure("\(op) is not a match op")
        }
    }

    func asyncMatch(_ op: Op, _ c: Concurrency) -> Pipe<Int, Fail> {
        let (n, o, b) = (c.concurrency, c.ordered, c.bufferLimit)
        switch op {
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
            default: preconditionFailure("\(op) is not a match op")
        }
    }
}

// MARK: - Matching failures

extension RealStage {
    func matchError(_ op: Op) -> Pipe<Int, Fail> {
        switch op {
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
            case .tapMatchError(let m):
                return TapMatchError(
                    where: { (f: Fail) in F.errorMatches(m, f) },
                    then: { (f: Fail) in log.add("\(i) tapMatchError \(f.code)") }
                ).attach(upstream)
            default: preconditionFailure("\(op) is not a matchError op")
        }
    }

    func asyncMatchError(_ op: Op, _ c: Concurrency) -> Pipe<Int, Fail> {
        let (n, o, b) = (c.concurrency, c.ordered, c.bufferLimit)
        switch op {
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
            case .tapMatchError(let m):
                return AsyncTapMatchError(
                    where: { (f: Fail) in F.errorMatches(m, f) },
                    then: { (f: Fail) in
                        await jitter(f.code, salt)
                        log.add("\(i) tapMatchError \(f.code)")
                    }
                ).attach(upstream)
            default: preconditionFailure("\(op) is not a matchError op")
        }
    }
}

#endif

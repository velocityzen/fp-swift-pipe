// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe

// The builder bodies the builder property reads: the optional stages of
// `BuilderSlots`, in order, then an `if`/`else` and a `switch`. They are
// split into three bodies, chained, so no one body grows too long. Pipe and
// open-pipe bodies go through different builder overloads, so each has its
// own set. Keep `BuilderSlots.stages` in step with them.

/// A closed pipe from a source of results.
func builtPipe(_ source: LoggedSource, _ p: BuilderParameters, log: Log) -> Pipe<Int, Fail> {
    pipeSlots(source, p, log: log)
}

/// A pipe from a source that cannot fail, widened by its first stage.
func builtWidenedPipe(_ values: [Int], _ p: BuilderParameters, log: Log) -> Pipe<Int, Fail> {
    let source = Pipe<Int, Fail> {
        From(values)
        FlatMap { (v: Int) -> Element in .success(v) }
    }
    return pipeSlots(source, p, log: log)
}

/// Open pipes, applied to the source one after another.
func builtOpenPipe(_ source: LoggedSource, _ p: BuilderParameters, log: Log) -> Pipe<Int, Fail> {
    let first = firstOpenSlots(p, log: log)
    let middle = middleOpenSlots(p)
    let last = lastOpenSlots(p, log: log)
    return last(middle(first(source)))
}

// MARK: - Pipe bodies

/// The three pipe bodies, chained after `source`.
func pipeSlots<S: AsyncSequence & Sendable>(
    _ source: S,
    _ p: BuilderParameters,
    log: Log
) -> Pipe<Int, Fail> where S.Element == Element, S.Failure == Never {
    let first = firstPipeSlots(source, p, log: log)
    let middle = middlePipeSlots(first, p)
    return lastPipeSlots(middle, p, log: log)
}

/// Slots 0 to 8.
func firstPipeSlots<S: AsyncSequence & Sendable>(
    _ source: S,
    _ p: BuilderParameters,
    log: Log
) -> Pipe<Int, Fail> where S.Element == Element, S.Failure == Never {
    let (on, a, m, x, c, b) = (p.on, p.a, p.m, p.x, p.c, p.b)
    return Pipe<Int, Fail> {
        FromResult(source)
        if on[0] {
            Map { (v: Int) in F.map(a, v) }
        }
        if on[1] {
            Filter { (v: Int) in F.filter(m, v) }
        }
        if on[2] {
            CompactMap { (v: Int) in F.compactMap(m, v) }
        }
        if on[3] {
            FlatMap { (v: Int) in F.flatMap(m, v) }
        }
        if on[4] {
            Tap { (v: Int) in log.add("4 tap \(v)") }
        }
        if on[5] {
            MapError { (f: Fail) in F.mapError(a, f) }
        }
        if on[6] {
            AsyncMap(concurrency: c, ordered: true, bufferLimit: b) { (v: Int) in
                await jitter(v, 0)
                return F.map(a, v)
            }
        }
        if on[7] {
            Match(where: { (v: Int) in F.matches(m, v) }, then: F.matched)
        }
        if on[8] {
            Match(x, then: { F.matchedValue(x) })
        }
    }
}

/// Slots 9 to 17.
func middlePipeSlots(_ upstream: Pipe<Int, Fail>, _ p: BuilderParameters) -> Pipe<Int, Fail> {
    let (on, m, x, n, size) = (p.on, p.m, p.x, p.n, p.size)
    return Pipe<Int, Fail> {
        FromResult(upstream)
        if on[9] {
            Match(Int.self, then: { (v: Int) -> Element in F.matched(v) })
        }
        if on[10] {
            FlatMapError { (f: Fail) in F.flatMapError(m, f) }
        }
        if on[11] {
            MatchError(extract: { (f: Fail) in F.extract(m, f) }, then: F.extracted)
        }
        if on[12] {
            MatchError(Fail(code: x), then: { F.extracted(x) })
        }
        if on[13] {
            MatchError(Fail.self, then: { (f: Fail) -> Element in F.flatMapError(2, f) })
        }
        if on[14] {
            Alt { F.alt(x) }
        }
        if on[15] {
            Take(n)
        }
        if on[16] {
            Drop(n / 2)
        }
        if on[17] {
            FlatMapSequence(bufferSize: size) { (v: Int) in F.expand(v) }
        }
    }
}

/// Slots 18 to 23, then the `if`/`else` and the `switch`.
func lastPipeSlots(
    _ upstream: Pipe<Int, Fail>,
    _ p: BuilderParameters,
    log: Log
) -> Pipe<Int, Fail> {
    let (on, branch, a, m, x, size, c, b) = (p.on, p.branch, p.a, p.m, p.x, p.size, p.c, p.b)
    return Pipe<Int, Fail> {
        FromResult(upstream)
        if on[18] {
            FlatMapAsyncSequence(bufferSize: size) { (v: Int) in
                Repeated(value: v, count: F.expand(v).count)
            }
        }
        if on[19] {
            TapError { (f: Fail) in log.add("19 tapError \(f.code)") }
        }
        if on[20] {
            TapMatch(x, then: { log.add("20 tapMatch") })
        }
        if on[21] {
            TapMatch(Int.self, then: { (v: Int) in log.add("21 tapMatchType \(v)") })
        }
        if on[22] {
            TapMatchError(
                where: { (f: Fail) in F.errorMatches(m, f) },
                then: { (f: Fail) in log.add("22 tapMatchError \(f.code)") }
            )
        }
        if on[23] {
            AsyncFilter(concurrency: c, ordered: true, bufferLimit: b) { (v: Int) in
                await jitter(v, 1)
                return F.filter(m, v)
            }
        }
        if on[24] {
            Map { (v: Int) in F.map(a, v) }
        } else {
            Map { (v: Int) in F.map(a &+ 1, v) }
        }
        switch branch {
            case 0:
                AsyncFlatMapError { (f: Fail) in
                    await jitter(f.code, 2)
                    return F.flatMapError(m, f)
                }
            case 1:
                AsyncFlatMapError(concurrency: c, ordered: true, bufferLimit: b) { (f: Fail) in
                    await jitter(f.code, 2)
                    return F.flatMapError(m, f)
                }
            default:
                AsyncFlatMapError(concurrency: c, ordered: true, bufferLimit: b) { (f: Fail) in
                    await jitter(f.code, 2)
                    return F.flatMapError(2, f)
                }
        }
    }
}

// MARK: - Open-pipe bodies

/// Slots 0 to 8.
func firstOpenSlots(_ p: BuilderParameters, log: Log) -> OpenPipe<Element, Int, Fail> {
    let (on, a, m, x, c, b) = (p.on, p.a, p.m, p.x, p.c, p.b)
    return OpenPipe {
        FromResult(Int.self, Fail.self)
        if on[0] {
            Map { (v: Int) in F.map(a, v) }
        }
        if on[1] {
            Filter { (v: Int) in F.filter(m, v) }
        }
        if on[2] {
            CompactMap { (v: Int) in F.compactMap(m, v) }
        }
        if on[3] {
            FlatMap { (v: Int) in F.flatMap(m, v) }
        }
        if on[4] {
            Tap { (v: Int) in log.add("4 tap \(v)") }
        }
        if on[5] {
            MapError { (f: Fail) in F.mapError(a, f) }
        }
        if on[6] {
            AsyncMap(concurrency: c, ordered: true, bufferLimit: b) { (v: Int) in
                await jitter(v, 0)
                return F.map(a, v)
            }
        }
        if on[7] {
            Match(where: { (v: Int) in F.matches(m, v) }, then: F.matched)
        }
        if on[8] {
            Match(x, then: { F.matchedValue(x) })
        }
    }
}

/// Slots 9 to 17.
func middleOpenSlots(_ p: BuilderParameters) -> OpenPipe<Element, Int, Fail> {
    let (on, m, x, n, size) = (p.on, p.m, p.x, p.n, p.size)
    return OpenPipe {
        FromResult(Int.self, Fail.self)
        if on[9] {
            Match(Int.self, then: { (v: Int) -> Element in F.matched(v) })
        }
        if on[10] {
            FlatMapError { (f: Fail) in F.flatMapError(m, f) }
        }
        if on[11] {
            MatchError(extract: { (f: Fail) in F.extract(m, f) }, then: F.extracted)
        }
        if on[12] {
            MatchError(Fail(code: x), then: { F.extracted(x) })
        }
        if on[13] {
            MatchError(Fail.self, then: { (f: Fail) -> Element in F.flatMapError(2, f) })
        }
        if on[14] {
            Alt { F.alt(x) }
        }
        if on[15] {
            Take(n)
        }
        if on[16] {
            Drop(n / 2)
        }
        if on[17] {
            FlatMapSequence(bufferSize: size) { (v: Int) in F.expand(v) }
        }
    }
}

/// Slots 18 to 23, then the `if`/`else` and the `switch`.
func lastOpenSlots(_ p: BuilderParameters, log: Log) -> OpenPipe<Element, Int, Fail> {
    let (on, branch, a, m, x, size, c, b) = (p.on, p.branch, p.a, p.m, p.x, p.size, p.c, p.b)
    return OpenPipe {
        FromResult(Int.self, Fail.self)
        if on[18] {
            FlatMapAsyncSequence(bufferSize: size) { (v: Int) in
                Repeated(value: v, count: F.expand(v).count)
            }
        }
        if on[19] {
            TapError { (f: Fail) in log.add("19 tapError \(f.code)") }
        }
        if on[20] {
            TapMatch(x, then: { log.add("20 tapMatch") })
        }
        if on[21] {
            TapMatch(Int.self, then: { (v: Int) in log.add("21 tapMatchType \(v)") })
        }
        if on[22] {
            TapMatchError(
                where: { (f: Fail) in F.errorMatches(m, f) },
                then: { (f: Fail) in log.add("22 tapMatchError \(f.code)") }
            )
        }
        if on[23] {
            AsyncFilter(concurrency: c, ordered: true, bufferLimit: b) { (v: Int) in
                await jitter(v, 1)
                return F.filter(m, v)
            }
        }
        if on[24] {
            Map { (v: Int) in F.map(a, v) }
        } else {
            Map { (v: Int) in F.map(a &+ 1, v) }
        }
        switch branch {
            case 0:
                AsyncFlatMapError { (f: Fail) in
                    await jitter(f.code, 2)
                    return F.flatMapError(m, f)
                }
            case 1:
                AsyncFlatMapError(concurrency: c, ordered: true, bufferLimit: b) { (f: Fail) in
                    await jitter(f.code, 2)
                    return F.flatMapError(m, f)
                }
            default:
                AsyncFlatMapError(concurrency: c, ordered: true, bufferLimit: b) { (f: Fail) in
                    await jitter(f.code, 2)
                    return F.flatMapError(2, f)
                }
        }
    }
}

#endif

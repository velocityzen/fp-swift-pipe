// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

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
        let model = ModelStage(up: up, i: i, log: log)
        switch family {
            case .value: return model.value(self)
            case .failure: return model.failure(self)
            case .match: return model.match(self)
            case .matchError: return model.matchError(self)
        }
    }
}

/// What a model stage is built from: the pull before it, and its index and
/// the log for its side effects.
struct ModelStage {
    let up: Pull
    let i: Int
    let log: ModelLog

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
}

// MARK: - Values

extension ModelStage {
    func value(_ op: Op) -> Pull {
        switch op {
            case .map(let a): return onSuccess { .success(F.map(a, $0)) }
            case .filter(let m): return onSuccess { F.filter(m, $0) ? .success($0) : nil }
            case .compactMap(let m): return onSuccess { F.compactMap(m, $0).map(Element.success) }
            case .flatMap(let m): return onSuccess { F.flatMap(m, $0) }
            case .tap:
                return onSuccess { v in
                    log.entries.append("\(i) tap \(v)")
                    return .success(v)
                }
            case .take(let n): return take(n)
            case .drop(let n): return drop(n)
            case .flatMapSequence, .flatMapAsyncSequence: return expand()
            default: preconditionFailure("\(op) is not a value op")
        }
    }

    func take(_ n: Int) -> Pull {
        var remaining = n
        return {
            guard remaining > 0 else { return nil }
            remaining -= 1
            return up()
        }
    }

    func drop(_ n: Int) -> Pull {
        var remaining = n
        return {
            while remaining > 0 {
                remaining -= 1
                guard up() != nil else { return nil }
            }
            return up()
        }
    }

    /// `FlatMapSequence` and `FlatMapAsyncSequence`: each success becomes
    /// its copies, and failures pass through.
    func expand() -> Pull {
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

// MARK: - Failures

extension ModelStage {
    func failure(_ op: Op) -> Pull {
        switch op {
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
            default: preconditionFailure("\(op) is not a failure op")
        }
    }
}

// MARK: - Matching

extension ModelStage {
    func match(_ op: Op) -> Pull {
        switch op {
            case .match(let m): return onSuccess { F.matches(m, $0) ? F.matched($0) : .success($0) }
            case .matchValue(let x): return onSuccess { $0 == x ? F.matchedValue(x) : .success($0) }
            case .matchType: return onSuccess { F.matched($0) }
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
            default: preconditionFailure("\(op) is not a match op")
        }
    }

    func matchError(_ op: Op) -> Pull {
        switch op {
            case .matchError(let m):
                return onFailure { f in F.extract(m, f).map(F.extracted) ?? .failure(f) }
            case .matchErrorValue(let code):
                return onFailure { $0.code == code ? F.extracted(code) : .failure($0) }
            case .matchErrorType: return onFailure { F.flatMapError(2, $0) }
            case .tapMatchError(let m):
                return onFailure { f in
                    if F.errorMatches(m, f) { log.entries.append("\(i) tapMatchError \(f.code)") }
                    return .failure(f)
                }
            default: preconditionFailure("\(op) is not a matchError op")
        }
    }
}

#endif

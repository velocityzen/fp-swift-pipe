// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe

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

    var description: String {
        let input = self.input.map { element in
            switch element {
                case .success(let v): "\(v)"
                case .failure(let f): "Fail(\(f.code))"
            }
        }
        return
            "[\(input.joined(separator: ", "))] from \(source), yields \(sourceYields) → \(stages)"
    }
}

func realPipe(_ input: [Element], _ stages: [Stage], log: Log) -> Pipe<Int, Fail> {
    PipeSpec(input: input, stages: stages).pipe(log: log)
}

#endif

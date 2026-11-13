// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe
import HegelTesting
import Testing

extension PipeProperties {
    /// A slow reader holds back a concurrent stage. The stage reads from the
    /// source only while fewer than `concurrency` elements are being
    /// processed or wait for the reader. Ordered, results that wait behind a
    /// slower element don't count, but `bufferLimit` caps them. So once the
    /// reader has taken `k` elements, the source has given at most
    /// `k + concurrency`, or `k + concurrency + bufferLimit` when ordered.
    ///
    /// The stage gives one element for each it reads, so no element frees
    /// its slot by being dropped.
    @Test
    func slowReadersHoldBackTheSource() async throws {
        try await property(reaching: [Reach.heldBack]) { tc in
            let failureRate = try tc.draw(PipeGen.failureRate, named: "failureRate")
            let input = try tc.draw(PipeGen.input(failureRate: failureRate), named: "input")
            // Ordered with no `bufferLimit`, nothing limits how far the source
            // runs ahead, so those stages are left out.
            let stage = try tc.draw(
                Hegel.zip(
                    PipeGen.op.filter { $0.hasConcurrency && !$0.drops },
                    Gen.oneOf(PipeGen.parallel, PipeGen.tight).filter { sourceSlack($0) != nil },
                    .int()
                ).map { Stage(op: $0, async: $1, salt: $2) },
                named: "stage"
            )
            let source = try tc.draw(PipeGen.source, named: "source")
            // How many times the reader yields after taking each element, so
            // the stage can run ahead.
            let pause = try tc.draw(.oneOf(.int(in: 0...3), .int(in: 0...30)), named: "pause")
            let slack = sourceSlack(stage.async!)!

            let (taken, ahead) = try await tc.withStallDetection(after: stallLimit) { heartbeat in
                let log = Log(onProgress: { heartbeat.beat() })
                let pipe = stage.attach(
                    sourcePipe(LoggedSource(elements: input, log: log), source),
                    index: 0,
                    log: log
                )
                var taken = 0
                var ahead = log.reads
                for await _ in pipe {
                    taken += 1
                    for _ in 0..<pause { await Task.yield() }
                    ahead = max(ahead, log.reads - taken)
                }
                return (taken, ahead)
            }
            #expect(taken == input.count)
            #expect(ahead <= slack, "the source ran \(ahead) elements ahead of the reader")

            if ahead == slack, slack < input.count { try tc.event(Reach.heldBack) }
        }
    }
}

/// How far ahead of the reader the source may run through a concurrent
/// stage, or nil when nothing limits it: ordered, with no `bufferLimit`.
func sourceSlack(_ stage: Concurrency) -> Int? {
    guard stage.ordered else { return stage.concurrency }
    guard stage.bufferLimit != .max else { return nil }
    let (slack, overflow) = stage.concurrency.addingReportingOverflow(stage.bufferLimit)
    return overflow ? nil : slack
}

#endif

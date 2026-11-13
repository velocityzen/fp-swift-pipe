// Hegel runs on macOS and Linux only; see Package.swift.
#if os(macOS) || os(Linux)

import FPPipe
import HegelTesting
import Synchronization
import Testing

extension PipeProperties {
    /// Results wait for as long as they have to, however many there are.
    ///
    /// - Ordered, the results behind one slow element do not count toward
    ///   `concurrency`: every later element up to `bufferLimit` finishes
    ///   while the first one is still running. The stage then gives them
    ///   all in source order.
    /// - Unordered, up to `concurrency` results wait for a reader that has
    ///   not read yet. The stage then gives every element.
    ///
    /// A stage keeps waiting results in a buffer that makes room for some
    /// at first and grows when more wait. The menus aim at that room: a few
    /// times `concurrency`, and the preallocation limit of 1,024.
    @Test
    func resultsWaitHoweverManyThereAre() async throws {
        try await property(
            reaching: [Reach.waitedPastConcurrency, Reach.bufferGrew, Reach.grewPastPreallocation]
        ) { tc in
            let stage = try tc.draw(
                Hegel.zip(
                    .oneOf(
                        .int(in: 2...6),
                        .int(in: 1_020...1_030),
                        .int(in: 2...Int.max),
                        .just(.max)
                    ),
                    .bool(),
                    .oneOf(
                        .just(.max),
                        .int(in: 0...8),
                        .int(in: 1_020...1_030),
                        .int(in: 0...Int.max)
                    )
                ).map { Concurrency(concurrency: $0, ordered: $1, bufferLimit: $2) },
                named: "stage"
            )
            let count = try tc.draw(
                .oneOf(.int(in: 0...40), .int(in: 1_020...1_030)),
                named: "count"
            )
            let input = (0..<count).map { Element.success($0) }
            // Ordered, the results behind element 0, up to `bufferLimit`.
            // Unordered, the results for the reader, up to `concurrency`.
            let waiting =
                stage.ordered
                ? max(0, min(count - 1, stage.bufferLimit)) : min(count, stage.concurrency)

            // If fewer results can wait, element 0 or the reader waits for
            // ever, and the heartbeat stops.
            let got = try await tc.withStallDetection(after: stallLimit) { heartbeat in
                let finished = Mutex(0)
                let pipe = AsyncMap(
                    concurrency: stage.concurrency,
                    ordered: stage.ordered,
                    bufferLimit: stage.bufferLimit
                ) { (v: Int) in
                    if stage.ordered, v == 0 {
                        await waitFor { finished.withLock { $0 } >= waiting }
                    } else {
                        finished.withLock { $0 += 1 }
                        heartbeat.beat()
                    }
                    return v
                }.attach(sourcePipe(LoggedSource(elements: input, log: Log()), .fromResult))
                var iterator = pipe.makeAsyncIterator()
                if !stage.ordered {
                    await waitFor { finished.withLock { $0 } >= waiting }
                }
                var got: [Element] = []
                while let element = await iterator.next() {
                    got.append(element)
                }
                return got
            }
            if stage.ordered {
                #expect(got == input)
            } else {
                #expect(counts(got) == counts(input))
            }

            if stage.ordered, waiting >= stage.concurrency {
                try tc.event(Reach.waitedPastConcurrency)
            }
            // Ordered, the buffer holds element 0's place too.
            let held = stage.ordered ? waiting + 1 : waiting
            let room = firstRoom(stage)
            if held > room {
                try tc.event(Reach.bufferGrew)
                if room == 1_024 { try tc.event(Reach.grewPastPreallocation) }
            }
        }
    }
}

/// How many results a concurrent stage's buffer has room for before it
/// grows, as fp-swift 4.4.1 makes it (`SourceOrder` and `CompletionOrder` in
/// ConcurrentMap.swift). It makes room for the results the stage may hold,
/// at most 1,024 of them, rounded up to a power of 2. Ordered with no
/// `bufferLimit`, it guesses twice `concurrency` early results.
///
/// Only the reach events use this. If fp-swift sizes its buffers another
/// way, the events stop meaning what they say, but the checks still hold.
func firstRoom(_ stage: Concurrency) -> Int {
    let preallocationLimit = 1_024
    let waiting = min(stage.concurrency, preallocationLimit)
    var room = waiting
    if stage.ordered {
        let early =
            stage.bufferLimit == .max ? 2 * waiting : min(stage.bufferLimit, preallocationLimit)
        room = min(waiting + early, preallocationLimit)
    }
    return room <= 0 ? 0 : 1 << (Int.bitWidth - (room - 1).leadingZeroBitCount)
}

/// Waits until `condition` holds, or the task is cancelled. Nothing limits
/// how long: the caller's stall detection does.
private func waitFor(_ condition: () -> Bool) async {
    while !condition(), !Task.isCancelled {
        try? await Task.sleep(for: .microseconds(100), tolerance: .zero)
    }
}

#endif

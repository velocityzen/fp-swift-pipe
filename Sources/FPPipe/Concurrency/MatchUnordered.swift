import FP

// `AsyncMatch` / `AsyncMatchError` at `concurrency > 1` run on fp-swift's
// `compactMapAsyncUnordered`. Its `passthrough` and its transform each see the raw
// element, so a stage that tested its pattern in both would run the user's predicate
// or extractor twice per match. Instead each element is sorted once, on the task
// reading the upstream, and only a match's payload is handed to a worker.

/// An element sorted by a matcher: the payload its handler needs, or the element itself
/// when it passes through.
@usableFromInline
enum Matched<Payload, Element> {
    case matched(Payload)
    case unmatched(Element)

    /// The element, when it passes through — the `passthrough` for concurrent matching.
    @inlinable
    var unmatched: Element? {
        guard case .unmatched(let element) = self else { return nil }
        return element
    }
}

extension Matched: Sendable where Payload: Sendable, Element: Sendable {}

/// Sorts each element with the matcher, calling it exactly once per element.
@usableFromInline
struct MatchSortStep<Matcher: PipeMatcher>: PipeStep {
    @usableFromInline let matcher: Matcher

    @inlinable
    init(_ matcher: Matcher) {
        self.matcher = matcher
    }

    @inlinable
    func callAsFunction(
        _ element: Matcher.Subject
    ) -> Matched<Matcher.Payload, Matcher.Subject>? {
        if let payload = matcher(element) { .matched(payload) } else { .unmatched(element) }
    }
}

extension Pipe {
    /// The `concurrency > 1` form of `AsyncMatch` / `AsyncMatchError`: up to `concurrency`
    /// handlers run at once and results emit as they complete.
    ///
    /// Each element is sorted on the task reading the upstream, so the matcher runs once
    /// per element; unmatched elements pass straight through without a worker, and only
    /// a match's payload is handed to one.
    @inlinable
    func matchAsyncUnordered<Matcher: PipeMatcher>(
        concurrency: Int,
        _ step: AsyncMatchElementStep<Matcher>,
    ) -> Pipe where Matcher.Subject == Element, Matcher.Payload: Sendable {
        let sort = MatchSortStep(step.matcher)
        return .iterating {
            .init(
                StepSequence(self, step: sort)
                    .compactMapAsyncUnordered(
                        concurrency: concurrency,
                        passthrough: { $0.unmatched },
                        { sorted in await step.resolve(sorted) },
                    )
                    .makeAsyncIterator(),
            )
        }
    }
}

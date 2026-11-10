import FP

// With `concurrency` above 1, `AsyncMatch` and `AsyncMatchError` use fp-swift's ordered or
// unordered `compactMap`. Its `passthrough` and its transform both get the original element. If
// both checked the pattern, the user's `where:` or `extract:` closure would run twice for each
// match. So each element is checked once, first, and only the payload of a match goes to a
// child task.

/// An element after the matcher checked it: a payload, or the element itself.
@usableFromInline
enum Matched<Payload, Element> {
    case matched(Payload)
    case unmatched(Element)

    /// The element when it passes through, or `nil`.
    @inlinable
    var unmatched: Element? {
        guard case .unmatched(let element) = self else { return nil }
        return element
    }
}

extension Matched: Sendable where Payload: Sendable, Element: Sendable {}

/// Checks each element with the matcher, exactly once.
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
    /// Runs `AsyncMatch` and `AsyncMatchError` when `concurrency` is above 1.
    @inlinable
    func matchAsync<Matcher: PipeMatcher>(
        concurrency: Int,
        ordered: Bool,
        bufferLimit: Int,
        _ step: AsyncMatchElementStep<Matcher>,
    ) -> Pipe where Matcher.Subject == Element, Matcher.Payload: Sendable {
        let sort = MatchSortStep(step.matcher)
        return .iterating {
            StepSequence(self, step: sort).compactMapAsyncIterator(
                concurrency: concurrency,
                ordered: ordered,
                bufferLimit: bufferLimit,
                passthrough: { $0.unmatched },
                { sorted in await step.resolve(sorted) },
            )
        }
    }
}

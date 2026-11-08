/// A `switch` over the failure channel: each failure the pattern matches is replaced by
/// the `Result` its handler returns — recover with `.success`, re-fail (with another
/// error) with `.failure`. Successes and non-matching failures pass through unchanged,
/// so the value and failure types are preserved.
@usableFromInline
struct MatchErrorStage<Value: Sendable, Matcher: PipeMatcher>: PipeFlatErrorStage
where Matcher.Subject: Error & Sendable {
    @usableFromInline typealias F = Matcher.Subject
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

    @usableFromInline let matcher: Matcher
    @usableFromInline let handler: @Sendable (Matcher.Payload) -> Result<Value, F>

    @inlinable
    init(_ matcher: Matcher, _ handler: @escaping @Sendable (Matcher.Payload) -> Result<Value, F>) {
        self.matcher = matcher
        self.handler = handler
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = MatchElementStep(FailureMatcher<Value, Matcher>(matcher), handler)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// DSL: `MatchError(AppError.rateLimited) { .success(Article.placeholder) }` — replaces each
/// failure equal to `error`.
@inlinable
public func MatchError<Value: Sendable, F: Error & Equatable & Sendable>(
    _ error: F,
    then handler: @escaping @Sendable () -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    MatchErrorStage(EqualMatcher(error), handler)
}

/// DSL: `MatchError(URLError.self) { e -> Result<Data, any Error> in .success(cached) }` —
/// replaces each failure that is an `E`, handing the handler the downcast error.
///
/// Only the handler's return type names the pipe's value and failure types, so spell it
/// out unless the body already does.
@inlinable
public func MatchError<Value: Sendable, F: Error & Sendable, E: Error>(
    _ type: E.Type,
    then handler: @escaping @Sendable (E) -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    MatchErrorStage(CastMatcher<F, E>(), handler)
}

/// DSL: `MatchError(where: \AppError.isRetryable) { e -> Result<Item, AppError> in … }` —
/// replaces each failure the predicate accepts.
@inlinable
public func MatchError<Value: Sendable, F: Error & Sendable>(
    where predicate: @escaping @Sendable (F) -> Bool,
    then handler: @escaping @Sendable (F) -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    MatchErrorStage(PredicateMatcher(predicate), handler)
}

/// DSL: `MatchError(extract: \AppError.statusCode) { code in .success(Page.error(code)) }` —
/// replaces each failure the extractor returns a payload for (typically one enum case's
/// associated value), handing the handler that payload.
@inlinable
public func MatchError<Value: Sendable, F: Error & Sendable, Payload>(
    extract: @escaping @Sendable (F) -> Payload?,
    then handler: @escaping @Sendable (Payload) -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    MatchErrorStage(ExtractMatcher(extract), handler)
}

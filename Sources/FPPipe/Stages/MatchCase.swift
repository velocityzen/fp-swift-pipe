/// A `switch` over the success channel: each success the pattern matches is replaced by
/// the `Result` its handler returns — keep or swap the value with `.success`, re-fail
/// with `.failure`. Failures and non-matching successes pass through unchanged, so the
/// value and failure types are preserved.
///
/// Not to be confused with `Match(onSuccess:onFailure:)`, which folds *every* element of
/// both channels into a new type.
@usableFromInline
struct MatchCaseStage<Matcher: PipeMatcher, F: Error & Sendable>: PipeStage
where Matcher.Subject: Sendable {
    @usableFromInline typealias Value = Matcher.Subject
    @usableFromInline typealias Input = Value
    @usableFromInline typealias Output = Value
    @usableFromInline typealias Failure = F

    @usableFromInline let matcher: Matcher
    @usableFromInline let handler: @Sendable (Matcher.Payload) -> Result<Value, F>

    @inlinable
    init(_ matcher: Matcher, _ handler: @escaping @Sendable (Matcher.Payload) -> Result<Value, F>) {
        self.matcher = matcher
        self.handler = handler
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = MatchElementStep(SuccessMatcher<Matcher, F>(matcher), handler)
        return .iterating { .init(StepIterator(upstream.makeAsyncIterator(), step: step)) }
    }
}

/// DSL: `Match(Response.unauthorized) { .failure(AppError.auth) }` — replaces each success
/// equal to `value`.
@inlinable
public func Match<Value: Equatable & Sendable, F: Error & Sendable>(
    _ value: Value,
    then handler: @escaping @Sendable () -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    MatchCaseStage(EqualMatcher(value), handler)
}

/// DSL: `Match(Cat.self) { cat -> Result<any Animal, AppError> in .failure(.noCats) }` —
/// replaces each success that is a `T`, handing the handler the downcast value.
///
/// Only the handler's return type names the pipe's value and failure types, so spell it
/// out unless the body already does.
@inlinable
public func Match<Value: Sendable, T, F: Error & Sendable>(
    _ type: T.Type,
    then handler: @escaping @Sendable (T) -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    MatchCaseStage(CastMatcher<Value, T>(), handler)
}

/// DSL: `Match(where: { (n: Int) in n < 0 }, then: { n in .failure(AppError.negative(n)) })`
/// — replaces each success the predicate accepts.
@inlinable
public func Match<Value: Sendable, F: Error & Sendable>(
    where predicate: @escaping @Sendable (Value) -> Bool,
    then handler: @escaping @Sendable (Value) -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    MatchCaseStage(PredicateMatcher(predicate), handler)
}

/// DSL: `Match(extract: \Event.remoteError) { e in .failure(AppError.remote(e)) }` —
/// replaces each success the extractor returns a payload for (typically one enum case's
/// associated value), handing the handler that payload.
@inlinable
public func Match<Value: Sendable, Payload, F: Error & Sendable>(
    extract: @escaping @Sendable (Value) -> Payload?,
    then handler: @escaping @Sendable (Payload) -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    MatchCaseStage(ExtractMatcher(extract), handler)
}

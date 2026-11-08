/// Asynchronous `Match(pattern)`: each success the pattern matches is replaced by the
/// `Result` its async handler returns. Failures and non-matching successes pass through.
///
/// `concurrency` controls how many handlers run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered), and
/// elements the pattern doesn't match pass straight through without a worker.
@usableFromInline
struct AsyncMatchCaseStage<Matcher: PipeMatcher, F: Error & Sendable>: PipeStage
where Matcher.Subject: Sendable, Matcher.Payload: Sendable {
    @usableFromInline typealias Value = Matcher.Subject
    @usableFromInline typealias Input = Value
    @usableFromInline typealias Output = Value
    @usableFromInline typealias Failure = F

    @usableFromInline let matcher: Matcher
    @usableFromInline let handler: @Sendable (Matcher.Payload) async -> Result<Value, F>
    @usableFromInline let concurrency: Int

    @inlinable
    init(
        _ matcher: Matcher,
        concurrency: Int,
        _ handler: @escaping @Sendable (Matcher.Payload) async -> Result<Value, F>,
    ) {
        self.matcher = matcher
        self.handler = handler
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = AsyncMatchElementStep(SuccessMatcher<Matcher, F>(matcher), handler)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        return upstream.matchAsyncUnordered(concurrency: concurrency, step)
    }
}

/// DSL: `AsyncMatch(Response.unauthorized) { await session.refresh() }` — replaces each
/// success equal to `value`.
@inlinable
public func AsyncMatch<Value: Equatable & Sendable, F: Error & Sendable>(
    _ value: Value,
    concurrency: Int = 1,
    then handler: @escaping @Sendable () async -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    AsyncMatchCaseStage(EqualMatcher(value), concurrency: concurrency, handler)
}

/// DSL: `AsyncMatch(Thumbnail.self) { t async -> Result<any Asset, AppError> in … }` —
/// replaces each success that is a `T`, handing the handler the downcast value.
///
/// Only the handler's return type names the pipe's value and failure types, so spell it
/// out unless the body already does.
@inlinable
public func AsyncMatch<Value: Sendable, T: Sendable, F: Error & Sendable>(
    _ type: T.Type,
    concurrency: Int = 1,
    then handler: @escaping @Sendable (T) async -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    AsyncMatchCaseStage(CastMatcher<Value, T>(), concurrency: concurrency, handler)
}

/// DSL: `AsyncMatch(where: { (p: Page) in p.isStale }, then: { p in await refetch(p) })`
/// — replaces each success the predicate accepts.
@inlinable
public func AsyncMatch<Value: Sendable, F: Error & Sendable>(
    where predicate: @escaping @Sendable (Value) -> Bool,
    concurrency: Int = 1,
    then handler: @escaping @Sendable (Value) async -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    AsyncMatchCaseStage(PredicateMatcher(predicate), concurrency: concurrency, handler)
}

/// DSL: `AsyncMatch(extract: \Event.redirect) { url in await follow(url) }` — replaces each
/// success the extractor returns a payload for, handing the handler that payload.
@inlinable
public func AsyncMatch<Value: Sendable, Payload: Sendable, F: Error & Sendable>(
    extract: @escaping @Sendable (Value) -> Payload?,
    concurrency: Int = 1,
    then handler: @escaping @Sendable (Payload) async -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    AsyncMatchCaseStage(ExtractMatcher(extract), concurrency: concurrency, handler)
}

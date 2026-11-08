/// Asynchronous `MatchError`: each failure the pattern matches is replaced by the
/// `Result` its async handler returns. Successes and non-matching failures pass through.
///
/// `concurrency` controls how many handlers run in parallel. Default 1 is strictly
/// sequential. With `concurrency > 1` results emit as they complete (unordered), and
/// elements the pattern doesn't match pass straight through without a worker.
@usableFromInline
struct AsyncMatchErrorStage<Value: Sendable, Matcher: PipeMatcher>: PipeFlatErrorStage
where Matcher.Subject: Error & Sendable, Matcher.Payload: Sendable {
    @usableFromInline typealias F = Matcher.Subject
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

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
        let step = AsyncMatchElementStep(FailureMatcher<Value, Matcher>(matcher), handler)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        return upstream.matchAsyncUnordered(concurrency: concurrency, step)
    }
}

/// DSL: `AsyncMatchError(AppError.rateLimited) { await cache.fallback() }` — replaces each
/// failure equal to `error`.
@inlinable
public func AsyncMatchError<Value: Sendable, F: Error & Equatable & Sendable>(
    _ error: F,
    concurrency: Int = 1,
    then handler: @escaping @Sendable () async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncMatchErrorStage(EqualMatcher(error), concurrency: concurrency, handler)
}

/// DSL: `AsyncMatchError(URLError.self) { e async -> Result<Data, any Error> in … }` —
/// replaces each failure that is an `E`, handing the handler the downcast error.
///
/// Only the handler's return type names the pipe's value and failure types, so spell it
/// out unless the body already does.
@inlinable
public func AsyncMatchError<Value: Sendable, F: Error & Sendable, E: Error>(
    _ type: E.Type,
    concurrency: Int = 1,
    then handler: @escaping @Sendable (E) async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncMatchErrorStage(CastMatcher<F, E>(), concurrency: concurrency, handler)
}

/// DSL: `AsyncMatchError(where: \AppError.isRetryable) { e in await retry(e) }` — replaces
/// each failure the predicate accepts.
@inlinable
public func AsyncMatchError<Value: Sendable, F: Error & Sendable>(
    where predicate: @escaping @Sendable (F) -> Bool,
    concurrency: Int = 1,
    then handler: @escaping @Sendable (F) async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncMatchErrorStage(PredicateMatcher(predicate), concurrency: concurrency, handler)
}

/// DSL: `AsyncMatchError(extract: \AppError.retryAfter) { delay in await retry(after: delay) }`
/// — replaces each failure the extractor returns a payload for, handing the handler that
/// payload.
@inlinable
public func AsyncMatchError<Value: Sendable, F: Error & Sendable, Payload: Sendable>(
    extract: @escaping @Sendable (F) -> Payload?,
    concurrency: Int = 1,
    then handler: @escaping @Sendable (Payload) async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncMatchErrorStage(ExtractMatcher(extract), concurrency: concurrency, handler)
}

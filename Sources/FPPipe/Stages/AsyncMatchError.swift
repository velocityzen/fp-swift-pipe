/// The stage behind `AsyncMatchError(pattern) { … }`.
@usableFromInline
struct AsyncMatchErrorStage<Value: Sendable, Matcher: PipeMatcher>: PipeFlatErrorStage
where Matcher.Subject: Error & Sendable, Matcher.Payload: Sendable {
    @usableFromInline typealias F = Matcher.Subject
    @usableFromInline typealias InputFailure = F
    @usableFromInline typealias OutputFailure = F

    @usableFromInline let matcher: Matcher
    @usableFromInline let handler: @Sendable (Matcher.Payload) async -> Result<Value, F>
    @usableFromInline let concurrency: Int
    @usableFromInline let ordered: Bool
    @usableFromInline let bufferLimit: Int

    @inlinable
    init(
        _ matcher: Matcher,
        concurrency: Int,
        ordered: Bool,
        bufferLimit: Int,
        _ handler: @escaping @Sendable (Matcher.Payload) async -> Result<Value, F>,
    ) {
        self.matcher = matcher
        self.handler = handler
        precondition(concurrency >= 1, "concurrency must be at least 1")
        self.concurrency = concurrency
        self.ordered = ordered
        precondition(bufferLimit >= 0, "bufferLimit must be at least 0")
        self.bufferLimit = bufferLimit
    }

    @inlinable
    func attach(_ upstream: Pipe<Value, F>) -> Pipe<Value, F> {
        let step = AsyncMatchElementStep(FailureMatcher<Value, Matcher>(matcher), handler)
        if concurrency == 1 {
            return .iterating { .init(AsyncStepIterator(upstream.makeAsyncIterator(), step: step)) }
        }
        return upstream.matchAsync(
            concurrency: concurrency,
            ordered: ordered,
            bufferLimit: bufferLimit,
            step
        )
    }
}

/// Recovers from, or fails again on, each failure that equals `error`.
///
/// Other elements pass through unchanged. Up to `concurrency` handlers run at the same time, and
/// results come out in the order they finish, or in source order with `ordered: true`.
/// `bufferLimit` works as in `AsyncMap`. See <doc:PatternMatching>.
@inlinable
public func AsyncMatchError<Value: Sendable, F: Error & Equatable & Sendable>(
    _ error: F,
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    then handler: @escaping @Sendable () async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncMatchErrorStage(
        EqualMatcher(error),
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit,
        handler
    )
}

/// Recovers from, or fails again on, each failure that is an `E`.
///
/// The handler gets the error cast to `E`. Other elements pass through unchanged. Up to
/// `concurrency` handlers run at the same time, and results come out in the order they finish,
/// or in source order with `ordered: true`. `bufferLimit` works as in `AsyncMap`.
/// Write the handler's return type, like `e async -> Result<Data, any Error> in`.
/// See <doc:PatternMatching>.
@inlinable
public func AsyncMatchError<Value: Sendable, F: Error & Sendable, E: Error>(
    _ type: E.Type,
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    then handler: @escaping @Sendable (E) async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncMatchErrorStage(
        CastMatcher<F, E>(),
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit,
        handler
    )
}

/// Recovers from, or fails again on, each failure for which `predicate` returns `true`.
///
/// Other elements pass through unchanged. Up to `concurrency` handlers run at the same time, and
/// results come out in the order they finish, or in source order with `ordered: true`.
/// `bufferLimit` works as in `AsyncMap`. See <doc:PatternMatching>.
@inlinable
public func AsyncMatchError<Value: Sendable, F: Error & Sendable>(
    where predicate: @escaping @Sendable (F) -> Bool,
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    then handler: @escaping @Sendable (F) async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncMatchErrorStage(
        PredicateMatcher(predicate),
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit,
        handler
    )
}

/// Recovers from, or fails again on, each failure for which `extract` returns a payload.
///
/// The handler gets the payload. Other elements pass through unchanged. Up to `concurrency`
/// handlers run at the same time, and results come out in the order they finish, or in source
/// order with `ordered: true`. `bufferLimit` works as in `AsyncMap`. See <doc:PatternMatching>.
@inlinable
public func AsyncMatchError<Value: Sendable, F: Error & Sendable, Payload: Sendable>(
    extract: @escaping @Sendable (F) -> Payload?,
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    then handler: @escaping @Sendable (Payload) async -> Result<Value, F>,
) -> some PipeFlatErrorStage<Value, F, F> {
    AsyncMatchErrorStage(
        ExtractMatcher(extract),
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit,
        handler
    )
}

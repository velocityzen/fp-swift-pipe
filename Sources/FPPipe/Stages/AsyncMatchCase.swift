/// The stage behind `AsyncMatch(pattern) { … }`.
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
        let step = AsyncMatchElementStep(SuccessMatcher<Matcher, F>(matcher), handler)
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

/// Replaces each success that equals `value` with the `Result` of `handler`.
///
/// Other elements pass through unchanged. Up to `concurrency` handlers run at the same time, and
/// results come out in the order they finish, or in source order with `ordered: true`.
/// `bufferLimit` works as in `AsyncMap`. See <doc:PatternMatching>.
@inlinable
public func AsyncMatch<Value: Equatable & Sendable, F: Error & Sendable>(
    _ value: Value,
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    then handler: @escaping @Sendable () async -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    AsyncMatchCaseStage(
        EqualMatcher(value),
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit,
        handler
    )
}

/// Replaces each success that is a `T` with the `Result` of `handler`.
///
/// The handler gets the value cast to `T`. Other elements pass through unchanged. Up to
/// `concurrency` handlers run at the same time, and results come out in the order they finish,
/// or in source order with `ordered: true`. `bufferLimit` works as in `AsyncMap`.
/// Write the handler's return type, like `t async -> Result<any Asset, AppError> in`.
/// See <doc:PatternMatching>.
@inlinable
public func AsyncMatch<Value: Sendable, T: Sendable, F: Error & Sendable>(
    _ type: T.Type,
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    then handler: @escaping @Sendable (T) async -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    AsyncMatchCaseStage(
        CastMatcher<Value, T>(),
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit,
        handler
    )
}

/// Replaces each success for which `predicate` returns `true` with the `Result` of `handler`.
///
/// Other elements pass through unchanged. Up to `concurrency` handlers run at the same time, and
/// results come out in the order they finish, or in source order with `ordered: true`.
/// `bufferLimit` works as in `AsyncMap`. See <doc:PatternMatching>.
@inlinable
public func AsyncMatch<Value: Sendable, F: Error & Sendable>(
    where predicate: @escaping @Sendable (Value) -> Bool,
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    then handler: @escaping @Sendable (Value) async -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    AsyncMatchCaseStage(
        PredicateMatcher(predicate),
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit,
        handler
    )
}

/// Replaces each success for which `extract` returns a payload with the `Result` of `handler`.
///
/// The handler gets the payload. Other elements pass through unchanged. Up to `concurrency`
/// handlers run at the same time, and results come out in the order they finish, or in source
/// order with `ordered: true`. `bufferLimit` works as in `AsyncMap`. See <doc:PatternMatching>.
@inlinable
public func AsyncMatch<Value: Sendable, Payload: Sendable, F: Error & Sendable>(
    extract: @escaping @Sendable (Value) -> Payload?,
    concurrency: Int = 1,
    ordered: Bool = false,
    bufferLimit: Int = .max,
    then handler: @escaping @Sendable (Payload) async -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    AsyncMatchCaseStage(
        ExtractMatcher(extract),
        concurrency: concurrency,
        ordered: ordered,
        bufferLimit: bufferLimit,
        handler
    )
}

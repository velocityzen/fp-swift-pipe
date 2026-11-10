/// The stage behind `Match(pattern) { … }`.
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

/// Replaces each success that equals `value` with the `Result` of `handler`.
///
/// Other elements pass through unchanged. See <doc:PatternMatching>.
@inlinable
public func Match<Value: Equatable & Sendable, F: Error & Sendable>(
    _ value: Value,
    then handler: @escaping @Sendable () -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    MatchCaseStage(EqualMatcher(value), handler)
}

/// Replaces each success that is a `T` with the `Result` of `handler`.
///
/// The handler gets the value cast to `T`. Other elements pass through unchanged. Write the
/// handler's return type, like `cat -> Result<any Animal, AppError> in`. See <doc:PatternMatching>.
@inlinable
public func Match<Value: Sendable, T, F: Error & Sendable>(
    _ type: T.Type,
    then handler: @escaping @Sendable (T) -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    MatchCaseStage(CastMatcher<Value, T>(), handler)
}

/// Replaces each success for which `predicate` returns `true` with the `Result` of `handler`.
///
/// Other elements pass through unchanged. See <doc:PatternMatching>.
@inlinable
public func Match<Value: Sendable, F: Error & Sendable>(
    where predicate: @escaping @Sendable (Value) -> Bool,
    then handler: @escaping @Sendable (Value) -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    MatchCaseStage(PredicateMatcher(predicate), handler)
}

/// Replaces each success for which `extract` returns a payload with the `Result` of `handler`.
///
/// The handler gets the payload. Other elements pass through unchanged. See <doc:PatternMatching>.
@inlinable
public func Match<Value: Sendable, Payload, F: Error & Sendable>(
    extract: @escaping @Sendable (Value) -> Payload?,
    then handler: @escaping @Sendable (Payload) -> Result<Value, F>,
) -> some PipeStage<Value, Value, F> {
    MatchCaseStage(ExtractMatcher(extract), handler)
}

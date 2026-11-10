// The steps behind the pattern-matching stages. A step checks whole `Result` elements with
// `SuccessMatcher` or `FailureMatcher`, so one step works for both channels.

/// Replaces each element that matches with the result of the handler.
@usableFromInline
struct MatchElementStep<Matcher: PipeMatcher>: PipeStep {
    @usableFromInline let matcher: Matcher
    @usableFromInline let handler: @Sendable (Matcher.Payload) -> Matcher.Subject

    @inlinable
    init(_ matcher: Matcher, _ handler: @escaping @Sendable (Matcher.Payload) -> Matcher.Subject) {
        self.matcher = matcher
        self.handler = handler
    }

    @inlinable
    func callAsFunction(_ element: Matcher.Subject) -> Matcher.Subject? {
        guard let payload = matcher(element) else { return element }
        return handler(payload)
    }
}

/// Runs the action for each element that matches. Every element passes through.
@usableFromInline
struct TapElementStep<Matcher: PipeMatcher>: PipeStep {
    @usableFromInline let matcher: Matcher
    @usableFromInline let action: @Sendable (Matcher.Payload) -> Void

    @inlinable
    init(_ matcher: Matcher, _ action: @escaping @Sendable (Matcher.Payload) -> Void) {
        self.matcher = matcher
        self.action = action
    }

    @inlinable
    func callAsFunction(_ element: Matcher.Subject) -> Matcher.Subject? {
        if let payload = matcher(element) {
            action(payload)
        }
        return element
    }
}

/// `MatchElementStep` with an async handler.
@usableFromInline
struct AsyncMatchElementStep<Matcher: PipeMatcher>: AsyncPipeStep {
    @usableFromInline let matcher: Matcher
    @usableFromInline let handler: @Sendable (Matcher.Payload) async -> Matcher.Subject

    @inlinable
    init(
        _ matcher: Matcher,
        _ handler: @escaping @Sendable (Matcher.Payload) async -> Matcher.Subject,
    ) {
        self.matcher = matcher
        self.handler = handler
    }

    @inlinable
    func callAsFunction(_ element: Matcher.Subject) async -> Matcher.Subject? {
        guard let payload = matcher(element) else { return element }
        return await handler(payload)
    }

    /// The result of the handler for a match, or the element itself.
    @inlinable
    func resolve(_ sorted: Matched<Matcher.Payload, Matcher.Subject>) async -> Matcher.Subject {
        switch sorted {
            case .matched(let payload): await handler(payload)
            case .unmatched(let element): element
        }
    }
}

/// `TapElementStep` with an async action.
@usableFromInline
struct AsyncTapElementStep<Matcher: PipeMatcher>: AsyncPipeStep {
    @usableFromInline let matcher: Matcher
    @usableFromInline let action: @Sendable (Matcher.Payload) async -> Void

    @inlinable
    init(_ matcher: Matcher, _ action: @escaping @Sendable (Matcher.Payload) async -> Void) {
        self.matcher = matcher
        self.action = action
    }

    @inlinable
    func callAsFunction(_ element: Matcher.Subject) async -> Matcher.Subject? {
        if let payload = matcher(element) {
            await action(payload)
        }
        return element
    }
}

/// Picks out the elements a `Match` / `MatchError` / `TapMatch` / `TapMatchError` stage
/// acts on: the payload its handler receives when `subject` matches, or `nil` when it
/// doesn't and the element passes through untouched.
///
/// Each way of spelling a pattern in the DSL — an `Equatable` value, a type, a predicate,
/// or a payload extractor — is a concrete matcher the stages are generic over, so the
/// test inlines into the stage's step like any other step body.
@usableFromInline
protocol PipeMatcher<Subject, Payload>: Sendable {
    associatedtype Subject
    associatedtype Payload

    func callAsFunction(_ subject: Subject) -> Payload?
}

/// Matches subjects equal to `expected`. The handler gets nothing — the caller already
/// knows the value it matched.
@usableFromInline
struct EqualMatcher<Subject: Equatable & Sendable>: PipeMatcher {
    @usableFromInline let expected: Subject

    @inlinable
    init(_ expected: Subject) {
        self.expected = expected
    }

    @inlinable
    func callAsFunction(_ subject: Subject) -> Void? {
        subject == expected ? () : nil
    }
}

/// Matches subjects whose dynamic type is `Payload` (or a subtype of it), handing the
/// handler the downcast value.
@usableFromInline
struct CastMatcher<Subject, Payload>: PipeMatcher {
    @inlinable
    init() {}

    @inlinable
    func callAsFunction(_ subject: Subject) -> Payload? {
        subject as? Payload
    }
}

/// Matches subjects the predicate accepts, handing the handler the subject itself.
@usableFromInline
struct PredicateMatcher<Subject>: PipeMatcher {
    @usableFromInline let predicate: @Sendable (Subject) -> Bool

    @inlinable
    init(_ predicate: @escaping @Sendable (Subject) -> Bool) {
        self.predicate = predicate
    }

    @inlinable
    func callAsFunction(_ subject: Subject) -> Subject? {
        predicate(subject) ? subject : nil
    }
}

/// Matches subjects the extractor returns a payload for — typically one enum case,
/// with the handler receiving its associated value.
@usableFromInline
struct ExtractMatcher<Subject, Payload>: PipeMatcher {
    @usableFromInline let extract: @Sendable (Subject) -> Payload?

    @inlinable
    init(_ extract: @escaping @Sendable (Subject) -> Payload?) {
        self.extract = extract
    }

    @inlinable
    func callAsFunction(_ subject: Subject) -> Payload? {
        extract(subject)
    }
}

// MARK: - Channel lifts
//
// A stage matches inside one channel of the pipe's `Result` elements. Lifting its
// matcher onto the whole element lets one step serve the success-side stages (`Match`,
// `TapMatch`) and the failure-side ones (`MatchError`, `TapMatchError`) alike.

/// Matches `.success` elements whose value `base` matches.
@usableFromInline
struct SuccessMatcher<Base: PipeMatcher, F: Error>: PipeMatcher {
    @usableFromInline let base: Base

    @inlinable
    init(_ base: Base) {
        self.base = base
    }

    @inlinable
    func callAsFunction(_ element: Result<Base.Subject, F>) -> Base.Payload? {
        guard case .success(let value) = element else { return nil }
        return base(value)
    }
}

/// Matches `.failure` elements whose error `base` matches.
@usableFromInline
struct FailureMatcher<Value, Base: PipeMatcher>: PipeMatcher where Base.Subject: Error {
    @usableFromInline let base: Base

    @inlinable
    init(_ base: Base) {
        self.base = base
    }

    @inlinable
    func callAsFunction(_ element: Result<Value, Base.Subject>) -> Base.Payload? {
        guard case .failure(let error) = element else { return nil }
        return base(error)
    }
}

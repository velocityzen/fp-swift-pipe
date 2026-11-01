// The unordered `Async*` stages run at `concurrency > 1` on fp-swift's
// `compactMapAsyncUnordered`, whose `passthrough` emits an element straight
// away, without a worker: these are the passthroughs for the channel a stage
// leaves untouched (e.g. failures through `AsyncMap`).

extension Result {
    /// The failure re-typed under a new success type, or `nil` for a success — the
    /// `passthrough` for stages that only transform successes.
    @inlinable
    func passthroughFailure<NewSuccess>() -> Result<NewSuccess, Failure>? {
        guard case .failure(let error) = self else { return nil }
        return .failure(error)
    }

    /// The success re-typed under a new failure type, or `nil` for a failure — the
    /// `passthrough` for stages that only transform failures.
    @inlinable
    func passthroughSuccess<NewFailure>() -> Result<Success, NewFailure>? {
        guard case .success(let value) = self else { return nil }
        return .success(value)
    }
}

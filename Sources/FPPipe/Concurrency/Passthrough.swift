// With `concurrency` above 1, the `Async*` stages use fp-swift's ordered or unordered
// `compactMap` (see `compactMapAsyncIterator`). Its `passthrough` passes an element on at once,
// with no worker task. These functions are the `passthrough` for the side a stage does not
// change, for example failures in `AsyncMap`.

extension Result {
    /// Returns the failure with a new success type, or `nil` for a success.
    @inlinable
    func passthroughFailure<NewSuccess>() -> Result<NewSuccess, Failure>? {
        guard case .failure(let error) = self else { return nil }
        return .failure(error)
    }

    /// Returns the success with a new failure type, or `nil` for a failure.
    @inlinable
    func passthroughSuccess<NewFailure>() -> Result<Success, NewFailure>? {
        guard case .success(let value) = self else { return nil }
        return .success(value)
    }
}

/// The first step of a pipe, like `From`. It puts elements into the pipe.
public protocol PipeSource<Output, Failure>: Sendable {
    associatedtype Output: Sendable
    associatedtype Failure: Error & Sendable

    func produce() -> Pipe<Output, Failure>
}

/// A node that seeds a pipeline with values.
public protocol PipeSource<Output, Failure>: Sendable {
    associatedtype Output: Sendable
    associatedtype Failure: Error & Sendable

    func produce() -> Pipe<Output, Failure>
}

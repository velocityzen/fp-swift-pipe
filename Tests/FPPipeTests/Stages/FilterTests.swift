@testable import FPPipe
import Testing

private enum AppError: Error, Equatable { case bad }

@Test
func filterKeepsMatchingSuccessesAndPassesFailures() async {
    let pipe = Pipe<Int, AppError> {
        From([1, 2, 3, 4, 5])
        FlatMap { (n: Int) -> Result<Int, AppError> in
            n == 3 ? .failure(.bad) : .success(n)
        }
        Filter { (n: Int) in n.isMultiple(of: 2) }
    }

    var observed: [Result<Int, AppError>] = []
    for await element in pipe {
        observed.append(element)
    }

    #expect(observed == [.success(2), .failure(.bad), .success(4)])
}

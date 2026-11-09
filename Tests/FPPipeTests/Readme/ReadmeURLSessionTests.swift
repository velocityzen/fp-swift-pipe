// The examples that call URLSession, from "Calling throwing code" and "A complete
// example", as Readme.md and the DocC articles in Sources/FPPipe/FPPipe.docc show them.
// When you change an example, change it in all three places.
//
// They run on Apple platforms only. Linux's URLSession does not load file URLs, which the
// complete example reads here instead of the network.

#if canImport(Darwin)
import FP
import FPPipe
import Foundation
import Testing

// MARK: - Calling throwing code

private struct User: Decodable, Sendable {
    let name: String
}

private enum LoadError: Error {
    case network(any Error)
    case decode(any Error)
}

// MARK: - A complete example

private enum ArticleError: Error, Sendable {
    case network(any Error)
    case decode(Data)
    case rateLimited

    var isRateLimited: Bool {
        if case .rateLimited = self { true } else { false }
    }
}

private struct Article: Decodable, Sendable {
    let id: String
    let isInteresting: Bool

    static let placeholder = Article(id: "-", isInteresting: false)
}

private func fetchArticles(
    _ urls: [URL]
) async -> Result<[Article], ArticleError> {
    let pipe = Pipe<Article, ArticleError> {
        // 1. Read the URLs.
        From(urls)

        // 2. Download up to 4 at a time, and keep the source order.
        AsyncFlatMapKeepOrder(concurrency: 4) { (url: URL) in
            await Result.fromAsync {
                try await URLSession.shared.data(from: url).0
            }
            .mapError(ArticleError.network)
        }

        // 3. Decode. A decode error becomes a failure.
        FlatMap { (data: Data) in
            Result {
                try JSONDecoder().decode(Article.self, from: data)
            }
            .mapError { _ in ArticleError.decode(data) }
        }

        // 4. Remove articles that are not interesting.
        Filter { (article: Article) in article.isInteresting }

        // 5. Log every failure.
        TapError { (error: ArticleError) in print("failed:", error) }

        // 6. Replace rate-limit failures with a placeholder.
        //    Other failures stay.
        MatchError(where: \ArticleError.isRateLimited) { _ in
            .success(Article.placeholder)
        }
    }

    // 7. Return all articles, or the first failure.
    return await pipe.toResult()
}

// MARK: - Tests

/// Writes each `(name, contents)` pair to a file in a new temporary directory.
private func writeFiles(_ files: [(String, String)]) throws -> (directory: URL, urls: [URL]) {
    let directory = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    let urls = try files.map { name, contents in
        let url = directory.appending(path: name)
        try Data(contents.utf8).write(to: url)
        return url
    }
    return (directory, urls)
}

@Test
func readmeCallingThrowingCode() async throws {
    let (directory, urls) = try writeFiles([
        ("ann.json", #"{"name":"ann"}"#),
        ("bad.json", "not json"),
    ])
    defer { try? FileManager.default.removeItem(at: directory) }

    let users = Pipe<User, LoadError> {
        From(urls)
        AsyncFlatMap { (url: URL) in
            await Result.fromAsync {
                try await URLSession.shared.data(from: url).0
            }
            .mapError(LoadError.network)
        }
        FlatMap { (data: Data) in
            Result { try JSONDecoder().decode(User.self, from: data) }
                .mapError(LoadError.decode)
        }
    }

    let results = await users.toArray()
    #expect(try results[0].get().name == "ann")
    guard case .failure(.decode) = results[1] else {
        Issue.record("expected a decode failure, got \(results[1])")
        return
    }
}

/// Foundation has its own `Pipe` type. Where both modules are imported, a `Pipe` in a
/// type position needs the module name.
private func names(_ users: [User]) -> FPPipe.Pipe<String, Never> {
    Pipe<String, Never> {
        From(users)
        Map { (user: User) in user.name }
    }
}

@Test
func readmeFoundationPipeNameNeedsTheModuleName() async {
    #expect(await names([User(name: "ann")]).toResult() == .success(["ann"]))
}

@Test
func readmeCompleteExample() async throws {
    let (directory, urls) = try writeFiles([
        ("a.json", #"{"id":"a","isInteresting":true}"#),
        ("b.json", #"{"id":"b","isInteresting":false}"#),
        ("c.json", #"{"id":"c","isInteresting":true}"#),
        ("bad.json", "not json"),
    ])
    defer { try? FileManager.default.removeItem(at: directory) }

    let articles = await fetchArticles(Array(urls.prefix(3)))
    #expect(try articles.get().map(\.id) == ["a", "c"])

    let failed = await fetchArticles([urls[0], urls[3]])
    guard case .failure(.decode) = failed else {
        Issue.record("expected a decode failure, got \(failed)")
        return
    }
}
#endif

# A Complete Example

A pipe that downloads, decodes, filters and recovers, all in one place.

This pipe downloads several articles at the same time and keeps their order. It decodes them, removes the ones that are not interesting, and recovers from rate limits:

```swift
import FP
import FPPipe
import Foundation

enum ArticleError: Error, Sendable {
    case network(any Error)
    case decode(Data)
    case rateLimited

    var isRateLimited: Bool {
        if case .rateLimited = self { true } else { false }
    }
}

struct Article: Decodable, Sendable {
    let id: String
    let isInteresting: Bool

    static let placeholder = Article(id: "-", isInteresting: false)
}

func fetchArticles(
    _ urls: [URL]
) async -> Result<[Article], ArticleError> {
    let pipe = Pipe<Article, ArticleError> {
        // 1. Read the URLs.
        From(urls)

        // 2. Download up to 4 at a time, and keep the source order.
        AsyncFlatMap(concurrency: 4, ordered: true) { (url: URL) in
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
```

<!-- Made from Readme.md by Scripts/make-docc-articles.swift. Edit Readme.md, then run the script. -->

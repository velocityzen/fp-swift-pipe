// Examples from "Async stages" and "Running work in parallel" in Readme.md and the DocC
// articles. Keep all three copies the same.

import FP
import FPPipe
import Foundation
import Testing

private struct User: Sendable, Equatable {
    let name: String
}

private struct APIError: Error, Equatable {}

private struct API: Sendable {
    func user(_ id: Int) async -> Result<User, APIError> { .success(User(name: "user \(id)")) }
    func isActive(_ user: User) async -> Bool { user.name != "user 2" }
}

private struct Item: Sendable {}

private func fetchData(_ url: URL) async -> Data { Data(url.lastPathComponent.utf8) }
private func decodeArticle(_ data: Data) async -> String {
    String(bytes: data, encoding: .utf8) ?? ""
}
private func isValid(_: Item) async -> Bool { true }

private let urls = (1...20).compactMap { URL(string: "https://example.com/\($0)") }

@Test
func readmeAsyncStages() async {
    let api = API()
    let userIDs = [1, 2, 3]

    let activeUsers = Pipe<User, APIError> {
        From(userIDs)
        AsyncFlatMap { (id: Int) in await api.user(id) }
        AsyncFilter { (user: User) in await api.isActive(user) }
    }

    #expect(await activeUsers.toResult() == .success([User(name: "user 1"), User(name: "user 3")]))
}

@Test
func readmeAsyncPipeOperator() async {
    let articles = Pipe<String, Never> {
        From(urls.prefix(2))
        AsyncMap { (url: URL) in await url |> fetchData |> decodeArticle }
    }

    #expect(await articles.toResult() == .success(["1", "2"]))
}

@Test
func readmeRunningWorkInParallel() async {
    let downloads = Pipe<Data, Never> {
        From(urls)
        AsyncMap(concurrency: 10) { (url: URL) in await fetchData(url) }
    }
    #expect(await downloads.toArray().count == urls.count)

    let items = Pipe<Item, Never> {
        From([Item(), Item(), Item()])
        AsyncFilter(concurrency: 4) { (item: Item) in await isValid(item) }
    }
    #expect(await items.toArray().count == 3)

    let ordered = Pipe<Data, Never> {
        From(urls)
        AsyncMap(concurrency: 10, ordered: true) { (url: URL) in
            await fetchData(url)
        }
    }
    #expect(await ordered.toResult() == .success(urls.map { Data($0.lastPathComponent.utf8) }))

    let limited = Pipe<Data, Never> {
        From(urls)
        AsyncMap(concurrency: 8, ordered: true, bufferLimit: 32) { (url: URL) in
            await fetchData(url)
        }
    }
    #expect(await limited.toResult() == ordered.toResult())
}

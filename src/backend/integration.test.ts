import { describe, expect, beforeAll, afterAll, it } from '@jest/globals'
import { KoboService } from './kobo/kobo.service'
import { NotionService } from './notion/notion.service'
import { LLMService } from './llm_integration/llm_integration.service'
import { env } from '../config/env.config'

describe('Kobo to Notion Workflow Integration', () => {
  let koboService: KoboService
  let notionService: NotionService
  let llmService: LLMService

  beforeAll(async () => {
    // Initialize all services
    koboService = new KoboService()
    notionService = new NotionService()
    llmService = new LLMService()

    // Connect to Kobo database
    await koboService.connect()
  })

  afterAll(async () => {
    // Clean up connections
    await koboService.close()
  })

  it('should process a complete workflow: load bookmarks, sync to Notion, and add summary', async () => {
    // 1. Get books from Kobo
    const books = await koboService.getBooks()
    expect(books.length).toBeGreaterThan(0)

    // Take the first book for testing
    const testBook = books[0]
    console.log('Processing book:', testBook.bookTitle)

    // 2. Get bookmarks for the test book
    const bookmarks = await koboService.getBookmarks(testBook.bookTitle)
    expect(bookmarks.length).toBeGreaterThan(0)
    console.log(`Found ${bookmarks.length} bookmarks`)

    // 3. Create or get Notion pages
    const { parentPageId, highlightPageId } = await notionService.getOrCreatePage(testBook)
    expect(parentPageId).toBeTruthy()
    expect(highlightPageId).toBeTruthy()
    console.log('Created Notion pages:', { parentPageId, highlightPageId })

    // 4. Sync bookmarks to Notion
    await notionService.syncBookmarks(highlightPageId, bookmarks)
    console.log('Synced bookmarks to Notion')

    // 5. Generate and sync summary if enabled
    if (env.SUMMARIZE_ENABLED) {
      console.log('Generating summary...')
      const summary = await llmService.summarizeBookmarks(
        testBook.bookTitle,
        bookmarks,
        env.SUMMARIZE_LANGUAGE
      )
      expect(summary).toBeTruthy()
      expect(summary.length).toBeGreaterThan(0)
      console.log('Generated summary:', summary)

      // Sync summary to Notion
      await notionService.syncSummary(parentPageId, summary)
      console.log('Synced summary to Notion')
    }

    // Final verification
    expect(true).toBe(true) // If we got here without errors, the test passed
  }, 60000) // Increase timeout to 60s for the complete workflow

  // it('should handle books with long content appropriately', async () => {
  //     // Get all books
  //     const books = await koboService.getBooks();

  //     // Find a book with many bookmarks (if any)
  //     let bookWithManyBookmarks: { book: Book; bookmarks: Bookmark[]; } | null = null;
  //     for (const book of books) {
  //         const bookmarks = await koboService.getBookmarks(book.bookTitle);
  //         if (bookmarks.length > 10) { // Arbitrary threshold
  //             bookWithManyBookmarks = { book, bookmarks };
  //             break;
  //         }
  //     }

  //     if (bookWithManyBookmarks) {
  //         const { book, bookmarks } = bookWithManyBookmarks;
  //         console.log(`Testing book with ${bookmarks.length} bookmarks:`, book.bookTitle);

  //         // Process the book with many bookmarks
  //         const { parentPageId, highlightPageId } = await notionService.getOrCreatePage(book);
  //         await notionService.syncBookmarks(highlightPageId, bookmarks);

  //         if (env.SUMMARIZE_ENABLED) {
  //             const summary = await llmService.summarizeBookmarks(
  //                 book.bookTitle,
  //                 bookmarks,
  //                 env.SUMMARIZE_LANGUAGE
  //             );
  //             await notionService.syncSummary(parentPageId, summary);
  //         }

  //         expect(true).toBe(true);
  //     } else {
  //         console.log('No books with many bookmarks found, skipping test');
  //     }
  // }, 90000);
})

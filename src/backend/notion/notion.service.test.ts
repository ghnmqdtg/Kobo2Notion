import { describe, expect, beforeEach, it } from "@jest/globals";
import { NotionService } from "./notion.service";
import { Book, Bookmark } from "../models";

describe("NotionService Integration Tests", () => {
  let service: NotionService;

  beforeEach(() => {
    service = new NotionService();
  });

  const testBook: Book = {
    bookTitle: "Test Book - " + new Date().toISOString(), // Unique title to avoid conflicts
    subtitle: "Test Subtitle",
    author: "Test Author",
    publisher: "Test Publisher",
    isbn: "978-1234567890",
    series: null,
    seriesNumber: null,
    readPercent: 0.5,
    imageId: null,
    source: "kobo-store",
    contentType: null,
  };

  const testBookmarks: Bookmark[] = [
    {
      volumeId: "test-volume-id",
      highlight: "Test Highlight 1",
      annotation: null,
      createdOn: new Date().toISOString().split("T")[0],
      type: "highlight",
    },
    {
      volumeId: "test-volume-id",
      highlight: "Test Highlight 2",
      annotation: "Test Annotation",
      createdOn: new Date().toISOString().split("T")[0],
      type: "highlight",
    },
  ];

  describe("Notion API Integration", () => {
    it("should create a new book page and sync bookmarks", async () => {
      // Create a new page for the test book
      const { parentPageId, highlightPageId } =
        await service.getOrCreatePage(testBook);

      console.log("parentPageId", parentPageId);
      console.log("highlightPageId", highlightPageId);

      // Verify the page IDs are returned
      expect(parentPageId).toBeTruthy();
      expect(highlightPageId).toBeTruthy();

      // Sync bookmarks to the highlight page
      await service.syncBookmarks(highlightPageId, testBookmarks);

      // Test passed if no errors were thrown
      expect(true).toBe(true);
    }, 30000); // Increase timeout to 30s for API calls

    it("should update existing book page", async () => {
      // First create a page
      const { parentPageId, highlightPageId } =
        await service.getOrCreatePage(testBook);

      // Try to create/update the same book again
      const result = await service.getOrCreatePage(testBook);

      // Should return the same parent page ID
      expect(result.parentPageId).toBe(parentPageId);
      expect(result.highlightPageId).toBeTruthy();
      expect(result.highlightPageId).not.toBe(highlightPageId); // Should be a new highlights page
    }, 30000);
  });
});

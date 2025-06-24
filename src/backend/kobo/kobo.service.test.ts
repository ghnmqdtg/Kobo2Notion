import { describe, expect, beforeAll, afterAll, it } from "@jest/globals";
import { KoboService } from "./kobo.service";
import { open } from "sqlite";
import sqlite3 from "sqlite3";

// Mock data for testing
const mockBooks = [
  {
    bookTitle: "Test Book 1",
    subtitle: null,
    author: "Test Author 1",
    publisher: "Test Publisher 1",
    isbn: "978-1234567890",
    series: null,
    seriesNumber: null,
    readPercent: 0.5,
    imageId: null,
  },
  {
    bookTitle: "Test Book 2",
    subtitle: "Test Subtitle 2",
    author: "Test Author 2",
    publisher: "Test Publisher 2",
    isbn: "978-0987654321",
    series: "Test Series",
    seriesNumber: 1,
    readPercent: 0.75,
    imageId: null,
  },
];

const mockBookmarks = [
  {
    volumeId: "ContentIdOfTestBook1",
    highlight: "Test Highlight 1",
    annotation: null,
    createdOn: "2023-10-26",
    type: "highlight",
  },
  {
    volumeId: "ContentIdOfTestBook1",
    highlight: "Test Highlight 2",
    annotation: "Test Annotation 2",
    createdOn: "2023-10-27",
    type: "bookmark",
  },
];

describe("KoboService", () => {
  let service: KoboService;
  let db: any;

  beforeAll(async () => {
    // Create an in-memory database for testing
    db = await open({
      filename: ":memory:",
      driver: sqlite3.Database,
    });

    // Create the necessary tables and insert mock data
    await db.exec(`
        CREATE TABLE content (
            ContentId TEXT PRIMARY KEY,
            Title TEXT,
            Subtitle TEXT,
            Attribution TEXT,
            Publisher TEXT,
            ISBN TEXT,
            Series TEXT,
            SeriesNumber REAL,
            ___PercentRead REAL,
            ImageId TEXT,
            isDownloaded TEXT,
            Accessibility INTEGER,
            EntitlementId TEXT,
            DownloadUrl TEXT,
            IsAbridged TEXT
        );

        CREATE TABLE Bookmark (
            VolumeID TEXT,
            Text TEXT,
            Annotation TEXT,
            DateCreated TEXT,
            Type TEXT
        );
    `);

    // Insert mock books
    for (const book of mockBooks) {
      await db.run(
        `INSERT INTO content (ContentId, Title, Subtitle, Attribution, Publisher, ISBN, Series, SeriesNumber, ___PercentRead, ImageId, isDownloaded, Accessibility, EntitlementId, DownloadUrl, IsAbridged) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          `ContentIdOf${book.bookTitle.replace(/\s+/g, "")}`,
          book.bookTitle,
          book.subtitle,
          book.author,
          book.publisher,
          book.isbn,
          book.series,
          book.seriesNumber,
          book.readPercent,
          book.imageId,
          "true",
          1,
          "EntitlementId",
          "DownloadUrl",
          "false",
        ],
      );
    }

    // Insert mock bookmarks
    for (const bookmark of mockBookmarks) {
      await db.run(
        `INSERT INTO Bookmark (VolumeID, Text, Annotation, DateCreated, Type) VALUES (?, ?, ?, ?, ?)`,
        [
          bookmark.volumeId,
          bookmark.highlight,
          bookmark.annotation,
          bookmark.createdOn,
          bookmark.type,
        ],
      );
    }

    // Initialize the KoboService with the in-memory database
    service = new KoboService();
    service["db"] = db; // Directly set the 'db' property of the service
  });

  afterAll(async () => {
    await db.close();
  });

  describe("getBooks", () => {
    it("should retrieve all books from the database", async () => {
      const books = await service.getBooks();
      expect(books).toEqual(
        expect.arrayContaining([
          expect.objectContaining(mockBooks[0]),
          expect.objectContaining(mockBooks[1]),
        ]),
      );
      expect(books.length).toBe(2);
    });
  });

  describe("getBooks", () => {
    it("should retrieve all books with their bookmark counts", async () => {
      const books = await service.getBooks();
      expect(books).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ ...mockBooks[0], bookmarkCount: 2 }),
          expect.objectContaining({ ...mockBooks[1], bookmarkCount: 0 }),
        ]),
      );
      expect(books.length).toBe(2);
      expect(books[0].bookmarkCount).toBeDefined();
      expect(books[1].bookmarkCount).toBeDefined();
    });
  });

  describe("getBookmarks", () => {
    it("should retrieve bookmarks for a given book title", async () => {
      const bookmarks = await service.getBookmarks("Test Book 1");
      expect(bookmarks).toEqual(
        expect.arrayContaining([
          expect.objectContaining(mockBookmarks[0]),
          expect.objectContaining(mockBookmarks[1]),
        ]),
      );
      expect(bookmarks.length).toBe(2);
    });

    it("should throw an error if no content ID is found for the given title", async () => {
      await expect(service.getBookmarks("Nonexistent Book")).rejects.toThrow(
        "No content ID found for title: Nonexistent Book",
      );
    });
  });
});

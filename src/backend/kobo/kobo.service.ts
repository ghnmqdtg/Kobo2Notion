import sqlite3 from "sqlite3";
import { open, Database } from "sqlite";
import { Book, Bookmark } from "../models"; // Create a models.ts to define types
import { env } from "../../config/env.config";

export class KoboService {
  private db: Database | null = null;
  private sqlitePath: string;

  constructor() {
    this.sqlitePath = env.SQLITE_SOURCE;
  }

  async connect(): Promise<void> {
    try {
      this.db = await open({
        filename: this.sqlitePath,
        driver: sqlite3.Database,
      });
      console.info("Connected to Kobo SQLite database.");
    } catch (error) {
      console.error("Error connecting to Kobo database:", error);
      throw error; // Re-throw to handle it in the main process
    }
  }

  async getBooks(): Promise<Book[]> {
    if (!this.db) throw new Error("Database not connected.");

    const query = `
      SELECT DISTINCT
        c.Title AS bookTitle,
        c.Subtitle AS subtitle,
        c.Attribution AS author,
        c.Publisher AS publisher,
        c.ISBN AS isbn,
        c.Series AS series,
        c.SeriesNumber AS seriesNumber,
        c.___PercentRead AS readPercent,
        c.ImageId AS imageId,
        c.ContentId AS contentId
      FROM content AS c
      WHERE
        c.isDownloaded = 'true' AND
        c.Accessibility = 1 AND
        c.EntitlementId IS NOT NULL AND
        c.DownloadUrl IS NOT NULL AND
        c.IsAbridged = 'false'
    `;

    const books = await this.db.all<(Book & { contentId: string; })[]>(query);

    // Get bookmark counts in a single query for efficiency
    const bookmarkCounts = await this.db.all<{ volumeId: string; count: number; }[]>(`
      SELECT 
        CASE 
          WHEN INSTR(VolumeID, '!') > 0 
          THEN SUBSTR(VolumeID, 1, INSTR(VolumeID, '!') - 1)
          ELSE VolumeID
        END as volumeId, 
        COUNT(*) as count
      FROM Bookmark 
      GROUP BY 
        CASE 
          WHEN INSTR(VolumeID, '!') > 0 
          THEN SUBSTR(VolumeID, 1, INSTR(VolumeID, '!') - 1)
          ELSE VolumeID
        END
    `);

    const countMap = new Map(bookmarkCounts.map(b => [b.volumeId, b.count]));

    const booksWithCounts = books.map(book => {
      const cleanedContentId = book.contentId.split('!')[0];
      return {
        bookTitle: book.bookTitle,
        subtitle: book.subtitle,
        author: book.author,
        publisher: book.publisher,
        isbn: book.isbn,
        series: book.series,
        seriesNumber: book.seriesNumber,
        readPercent: book.readPercent,
        imageId: book.imageId,
        bookmarkCount: countMap.get(cleanedContentId) || 0
      };
    });

    console.info(`Retrieved data for ${booksWithCounts.length} books with bookmark counts`);
    return booksWithCounts;
  }

  async getBookmarks(title: string): Promise<Bookmark[]> {
    if (!this.db) throw new Error("Database not connected.");

    const contentIdResult = await this.db.get<{ contentId: string; }>(
      `SELECT c.ContentId AS contentId FROM content AS c WHERE c.Title = ?`,
      [title],
    );

    if (!contentIdResult) {
      throw new Error(`No content ID found for title: ${title}`);
    }

    console.log("contentIdResult", contentIdResult);

    const contentId = contentIdResult.contentId;
    console.log("contentId", contentId);

    // Clean the contentId: remove all the text after ! sign (including the ! sign)
    const cleanedContentId = contentId.split("!")[0];

    console.info(`Retrieving bookmarks for content ID: ${cleanedContentId}`);

    const bookmarks = await this.db.all<Bookmark[]>(
      `SELECT VolumeID AS volumeId, Text AS highlight, Annotation AS annotation, DateCreated AS createdOn, Type AS type FROM Bookmark WHERE VolumeID = ? ORDER BY DateCreated ASC`,
      [cleanedContentId],
    );

    console.log("bookmarks", bookmarks);
    return bookmarks;
  }

  // Add close connection method
  async close(): Promise<void> {
    if (this.db) {
      await this.db.close();
      console.info("Kobo database connection closed.");
    }
  }
}

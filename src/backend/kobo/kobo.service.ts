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
        c.ImageId AS imageId
      FROM content AS c
      WHERE
        c.isDownloaded = 'true' AND
        c.Accessibility = 1 AND
        c.EntitlementId IS NOT NULL AND
        c.DownloadUrl IS NOT NULL AND
        c.IsAbridged = 'false'
    `;
    const books = await this.db.all<Book[]>(query);
    console.info(`Retrieved data for ${books.length} books`);
    return books;
  }

  async getBookmarks(title: string): Promise<Bookmark[]> {
    if (!this.db) throw new Error("Database not connected.");

    const contentIdResult = await this.db.get<{ contentId: string; }>(
      `SELECT c.ContentId AS contentId FROM content AS c WHERE c.Title LIKE ?`,
      [`%${title}%`],
    );

    if (!contentIdResult) {
      throw new Error(`No content ID found for title: ${title}`);
    }

    const contentId = contentIdResult.contentId;

    // Clean the contentId: remove all the text after ! sign (including the ! sign)
    const cleanedContentId = contentId.split("!")[0];

    console.info(`Retrieving bookmarks for content ID: ${cleanedContentId}`);

    const bookmarks = await this.db.all<Bookmark[]>(
      `SELECT VolumeID AS volumeId, Text AS highlight, Annotation AS annotation, DateCreated AS createdOn, Type AS type FROM Bookmark WHERE VolumeID = ? ORDER BY DateCreated ASC`,
      [cleanedContentId],
    );

    console.log(bookmarks);
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

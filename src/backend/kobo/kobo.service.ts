import sqlite3 from "sqlite3";
import { open, Database } from "sqlite";
import { Book, BookContentType, BookSource, Bookmark } from "../models";
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

  private async getBookmarkCountMap(): Promise<Map<string, number>> {
    if (!this.db) throw new Error("Database not connected.");

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

    return new Map(bookmarkCounts.map(b => [b.volumeId, b.count]));
  }

  private mimeTypeToSource(mimeType: string): BookSource {
    if (mimeType === 'application/x-kobo-html+instapaper') return 'instapaper';
    if (mimeType === 'application/epub+zip' || mimeType === 'application/pdf') return 'external';
    return 'kobo-store';
  }

  private mimeTypeToContentType(mimeType: string): BookContentType {
    if (mimeType === 'application/epub+zip') return 'epub';
    if (mimeType === 'application/pdf') return 'pdf';
    return null;
  }

  async getBooks(): Promise<Book[]> {
    if (!this.db) throw new Error("Database not connected.");

    // Kobo store books: purchased and downloaded
    const storeQuery = `
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
        c.ContentId AS contentId,
        c.MimeType AS mimeType
      FROM content AS c
      WHERE
        c.ContentType = 6 AND
        c.BookTitle IS NULL AND
        c.isDownloaded = 'true' AND
        c.Accessibility = 1 AND
        c.EntitlementId IS NOT NULL AND
        c.DownloadUrl IS NOT NULL AND
        c.IsAbridged = 'false'
    `;

    // External sideloads (EPUBs, PDFs) — excludes Instapaper articles (no user notes support)
    const nonStoreQuery = `
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
        c.ContentId AS contentId,
        c.MimeType AS mimeType
      FROM content AS c
      WHERE
        c.ContentType = 6 AND
        c.BookTitle IS NULL AND
        c.MimeType IN ('application/epub+zip', 'application/pdf')
    `;

    const [storeBooks, nonStoreBooks] = await Promise.all([
      this.db.all<(Book & { contentId: string; mimeType: string; })[]>(storeQuery),
      this.db.all<(Book & { contentId: string; mimeType: string; })[]>(nonStoreQuery),
    ]);

    const countMap = await this.getBookmarkCountMap();

    const allBooks = [...storeBooks, ...nonStoreBooks];

    const booksWithCounts = allBooks.map(book => {
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
        bookmarkCount: countMap.get(cleanedContentId) || 0,
        source: this.mimeTypeToSource(book.mimeType),
        contentType: this.mimeTypeToContentType(book.mimeType),
      };
    });

    console.info(`Retrieved ${booksWithCounts.length} items (${storeBooks.length} store, ${nonStoreBooks.length} external)`);
    return booksWithCounts;
  }

  async getBookmarks(title: string): Promise<Bookmark[]> {
    if (!this.db) throw new Error("Database not connected.");

    const contentIdResult = await this.db.get<{ contentId: string; }>(
      `SELECT c.ContentId AS contentId FROM content AS c WHERE c.Title = ? AND c.ContentType = 6 AND c.BookTitle IS NULL`,
      [title],
    );

    if (!contentIdResult) {
      throw new Error(`No content ID found for title: ${title}`);
    }

    const contentId = contentIdResult.contentId;

    // Clean the contentId: strip both ! suffixes (store books) and # suffixes (sideloads)
    const cleanedContentId = contentId.split("!")[0].split("#")[0];

    console.info(`Retrieving bookmarks for content ID: ${cleanedContentId}`);

    const bookmarks = await this.db.all<Bookmark[]>(
      `SELECT VolumeID AS volumeId, Text AS highlight, Annotation AS annotation, DateCreated AS createdOn, Type AS type FROM Bookmark WHERE VolumeID LIKE ? || '%' ORDER BY DateCreated ASC`,
      [cleanedContentId],
    );

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

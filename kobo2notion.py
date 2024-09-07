# Load packages
import os
import json
import dotenv
import logging
import sqlite3
import requests
import pandas as pd
from tqdm import tqdm
from notion_client import Client
from utils import CustomFormatter

# Load environment variables
dotenv.load_dotenv()
# Set up logging
logger = logging.getLogger("kobo2notion")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


class Kobo2Notion:
    def __init__(self, sqlite_path, notion_api_key, notion_db_id):
        self.sqlite_path = sqlite_path
        self.connection = self.connect_to_sqlite()
        self.notion_client = Client(auth=notion_api_key)
        self.notion_db_id = notion_db_id
        self.bookmarks = None
        logger.info("Kobo2Notion instance initialized")

    def connect_to_sqlite(self):
        try:
            connection = sqlite3.connect(self.sqlite_path)
            logger.info(f"Connected to SQLite database: {self.sqlite_path}")
            return connection
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite: {e}")
            return None

    def get_books_data(self):
        query = """
            SELECT DISTINCT 
                c.Title AS 'Book Title',
                c.Subtitle,
                c.Attribution AS 'Author',
                c.Publisher,
                c.ISBN,
                c.Series,
                c.SeriesNumber,
                c.___PercentRead AS 'Read Percent',
                c.ImageId
            FROM content AS c 
            WHERE 
                c.isDownloaded = 'true' AND 
                c.Accessibility = 1 AND 
                c.EntitlementId IS NOT NULL AND 
                c.DownloadUrl IS NOT NULL AND 
                c.IsAbridged = 'false'
        """
        # Fetch the book data from the SQLite database
        books_data = pd.read_sql_query(query, self.connection)
        logger.debug(f"Books data: {books_data}")
        logger.info(f"Retrieved data for {len(books_data)} books")
        return books_data

    def load_bookmark(self, title, highlight_page_id):
        books_in_file = pd.read_sql(
            f"SELECT c.ContentId AS 'Content ID', c.Title AS 'Book Title' FROM content AS c WHERE c.Title LIKE '%{title}%'",
            self.connection,
        )
        logger.debug(f"Books in file: {books_in_file}")
        # Load the bookmark from the SQLite database
        bookmark_df = pd.read_sql(
            f"SELECT VolumeID AS 'Volume ID', Text AS 'Highlight', Annotation, DateCreated AS 'Created On', Type FROM Bookmark Where VolumeID = '{books_in_file.iloc[0]['Content ID']}' ORDER BY 4 ASC",
            self.connection,
        )
        logger.debug(f"Loaded {len(bookmark_df)} bookmarks for '{title}'")

        return bookmark_df

    def fetch_book_cover(self, book_title, isbn):
        logger.info(f"Fetching book cover for '{book_title}' (ISBN: {isbn})")
        response = requests.get(
            f"https://www.googleapis.com/books/v1/volumes?q={book_title}"
        )
        data = response.json()

        book_id = None
        for item in data.get("items", []):
            identifiers = item.get("volumeInfo", {}).get("industryIdentifiers", [])
            for identifier in identifiers:
                if (
                    identifier.get("type") == "ISBN_13"
                    and identifier.get("identifier") == isbn
                ):
                    book_id = item["id"]
                    break
            if book_id:
                break

        if not book_id and data.get("items"):
            # If no ISBN match found, use the first item
            book_id = data["items"][0]["id"]
            logger.warning(
                f"No exact ISBN match found for '{book_title}'. Using first result."
            )

        if not book_id:
            logger.warning(f"Could not find any book data for '{book_title}'")
            return None

        image_url = f"https://books.google.com/books/publisher/content/images/frontcover/{book_id}?fife=w1200-h1200"
        image_response = requests.get(image_url)

        if image_response.status_code != 200:
            logger.error(
                f"Failed to fetch image for '{book_title}': {image_response.status_code}"
            )
            return None

        logger.info(f"Successfully fetched book cover for '{book_title}'")
        return image_url

    def check_page_exists(self, book_title):
        logger.info(f"Checking if page exists for book: {book_title}")
        # Check if the page exists
        query = self.notion_client.databases.query(
            database_id=self.notion_db_id,
            filter={"property": "Title", "title": {"equals": book_title}},
        )
        exists = bool(query["results"])
        logger.info(f"Page for '{book_title}' exists: {exists}")
        return query["results"][0]["id"] if exists else None

    def create_notion_page(self, book, cover_url):
        logger.info(f"Creating new page for book: {book['Book Title']}")

        # Create main page
        new_page = self._create_main_page(book, cover_url)
        logger.debug(f"Created new page for '{book['Book Title']}': {new_page['id']}")

        # Create highlight page
        highlight_page = self._create_highlight_page(new_page["id"])
        logger.debug(
            f"Created highlight page for '{book['Book Title']}': {highlight_page['id']}"
        )

        return {
            "new_page_id": new_page["id"],
            "highlight_page_id": highlight_page["id"],
        }

    def get_or_create_page(self, book):
        cover_url = self.fetch_book_cover(book["Book Title"], book["ISBN"])
        existing_page_id = self.check_page_exists(book["Book Title"])

        if existing_page_id:
            return self._update_existing_page(existing_page_id, cover_url)
        else:
            return self._create_new_pages(book, cover_url)

    def _create_main_page(self, book, cover_url):
        properties = {
            "Title": {"title": [{"text": {"content": book["Book Title"]}}]},
            "Category": {"select": {"name": "Books"}},
            "Author": {"rich_text": [{"text": {"content": book["Author"]}}]},
            "Publisher": {"rich_text": [{"text": {"content": book["Publisher"]}}]},
            "ISBN": {"rich_text": [{"text": {"content": book["ISBN"]}}]},
            "Read Percent": {"number": book["Read Percent"]},
        }

        # Add sub title if it exists
        if book["Subtitle"] is not None:
            properties["Subtitle"] = {
                "rich_text": [{"text": {"content": book["Subtitle"]}}]
            }

        return self.notion_client.pages.create(
            parent={"database_id": self.notion_db_id},
            cover=self._get_cover_data(cover_url),
            icon=self._get_cover_data(cover_url),
            properties=properties,
        )

    def _create_highlight_page(self, parent_id):
        return self.notion_client.pages.create(
            parent={"type": "page_id", "page_id": parent_id},
            properties={"title": [{"text": {"content": "Highlights"}}]},
        )

    def _update_existing_page(self, page_id, cover_url):
        logger.info(f"Page already exists, id: {page_id}")

        # Update cover and icon
        self.notion_client.pages.update(
            page_id=page_id,
            cover=self._get_cover_data(cover_url),
            icon=self._get_cover_data(cover_url),
        )

        # Archive old highlight page and create a new one
        self._archive_old_highlights(page_id)
        highlight_page_id = self._create_highlight_page(page_id)

        return {
            "parent": page_id,
            "highlight": highlight_page_id["id"],
        }

    def _create_new_pages(self, book, cover_url):
        page_ids = self.create_notion_page(book, cover_url)
        logger.info(
            f"Created new pages for '{book['Book Title']}'. "
            f"Main page ID: {page_ids['new_page_id']}, "
            f"Highlight page ID: {page_ids['highlight_page_id']}"
        )
        return {
            "parent": page_ids["new_page_id"],
            "highlight": page_ids["highlight_page_id"],
        }

    def _get_cover_data(self, cover_url):
        return {
            "type": "external",
            "external": {"url": cover_url},
        }

    def _archive_old_highlights(self, page_id):
        original_highlights = self.notion_client.blocks.children.list(block_id=page_id)
        if original_highlights["results"]:
            self.notion_client.pages.update(
                page_id=original_highlights["results"][0]["id"], archived=True
            )

    def sync_bookmarks(self):
        logger.info("Starting bookmark synchronization")
        books_data = self.get_books_data()

        for _, book in books_data.iterrows():
            book_title = book["Book Title"]
            page_ids = self.get_or_create_page(book)
            bookmarks = self.load_bookmark(book_title, page_ids["highlight"])

            # Clean up bookmarks (whitespace and newlines)
            bookmarks["Highlight"] = (
                bookmarks["Highlight"]
                .astype(str)
                .str.strip()
                .str.replace("\n", "", regex=False)
            )

            # Prepare children blocks for batch update
            children_blocks = []
            for _, bookmark in bookmarks.iterrows():
                if bookmark["Type"] == "highlight":
                    content = bookmark["Highlight"]
                    block_type = "paragraph"
                else:  # Assuming it's an annotation
                    content = (
                        bookmark["Annotation"]
                        if bookmark["Annotation"] is not None
                        else ""
                    )
                    if bookmark["Highlight"] is not None:
                        content += "\n" + bookmark["Highlight"]
                    block_type = "quote"

                children_blocks.append(
                    {
                        "object": "block",
                        "type": block_type,
                        block_type: {
                            "rich_text": [
                                {"type": "text", "text": {"content": content}}
                            ]
                        },
                    }
                )

                # Send data in batches of 100 (Notion API limit)
                if len(children_blocks) == 100:
                    self._send_bookmark_batch(page_ids["highlight"], children_blocks)
                    children_blocks = []

            # Send any remaining blocks
            if children_blocks:
                self._send_bookmark_batch(page_ids["highlight"], children_blocks)

            logger.info(f"Synced {len(bookmarks)} bookmarks for '{book_title}'")

        logger.info("Bookmark synchronization completed")

    def _send_bookmark_batch(self, highlight_page_id, children_blocks):
        try:
            self.notion_client.blocks.children.append(
                block_id=highlight_page_id,
                children=children_blocks,
            )
        except Exception as e:
            logger.error(f"Error syncing bookmarks to Notion: {e}")


if __name__ == "__main__":
    # Check if the environment variables are loaded
    assert os.environ["NOTION_API_KEY"] is not None, "NOTION_API_KEY is not set"
    assert os.environ["NOTION_DB_ID"] is not None, "NOTION_DB_ID is not set"
    # Check if it's in dev mode
    if os.environ["DEV_MODE"] == "true":
        DEV_MODE = True
    else:
        DEV_MODE = False
    # Copy the KoboReader.sqlite file to temp/KoboReader.sqlite
    # This is to avoid conflicts with the original file
    # Create a temp directory if it doesn't exist
    os.makedirs("temp", exist_ok=True)
    # Copy the KoboReader.sqlite file to temp/KoboReader.sqlite
    os.system(f"cp {os.environ['SQLITE_SOURCE']} temp/KoboReader.sqlite")

    logger.info("Initializing Kobo2Notion instance")
    kobo2notion = Kobo2Notion(
        sqlite_path="temp/KoboReader.sqlite",
        notion_api_key=os.environ["NOTION_API_KEY"],
        notion_db_id=os.environ["NOTION_DB_ID"],
    )
    kobo2notion.sync_bookmarks()
    logger.info("Kobo2Notion script completed")

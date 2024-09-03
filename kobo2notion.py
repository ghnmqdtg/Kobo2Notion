# Load packages
import os
import json
import dotenv
import logging
import sqlite3
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

    def get_book_titles(self):
        query = """
            SELECT DISTINCT 
                c.Title AS 'Book Title',
                c.isDownloaded, 
                c.RestOfBookEstimate, 
                c.EntitlementId, 
                c.DownloadUrl, 
                c.IsAbridged 
            FROM content AS c 
            WHERE 
                c.isDownloaded = 'true' AND 
                c.RestOfBookEstimate != 0 AND 
                c.EntitlementId IS NOT NULL AND 
                c.DownloadUrl IS NOT NULL AND 
                c.IsAbridged = 'false'
        """
        # Fetch the book titles from the SQLite database
        books_in_file = pd.read_sql_query(query, self.connection)
        titles = [title for title in books_in_file["Book Title"] if title is not None]
        logger.info(f"Retrieved {len(titles)} book titles")
        logger.debug(f"Book titles: {titles}")
        return titles

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

    def write_text(self, page_id, text, type):
        try:
            self.notion_client.blocks.children.append(
                block_id=page_id,
                children=[
                    {
                        "object": "block",
                        "type": type,
                        type: {
                            "rich_text": [{"type": "text", "text": {"content": text}}]
                        },
                    }
                ],
            )
        except Exception as e:
            print(e)

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

    def create_notion_page(self, book_title):
        logger.info(f"Creating new page for book: {book_title}")
        # Create a new page
        new_page = self.notion_client.pages.create(
            parent={"database_id": self.notion_db_id},
            properties={"Title": {"title": [{"text": {"content": book_title}}]}},
        )
        logger.debug(f"Created new page for '{book_title}': {new_page['id']}")

        # Create a new highlight page
        highlight_page = self.notion_client.pages.create(
            parent={"type": "page_id", "page_id": new_page["id"]},
            properties={"title": [{"text": {"content": "Highlights"}}]},
        )
        logger.debug(
            f"Created highlight page for '{book_title}': {highlight_page['id']}"
        )

        return {
            "new_page_id": new_page["id"],
            "highlight_page_id": highlight_page["id"],
        }

    def get_or_create_page(self, book_title):
        existing_page_id = self.check_page_exists(book_title)
        if existing_page_id:
            logger.info(
                f"Page for '{book_title}' already exists, id: {existing_page_id}"
            )
            # Delete the highlight page if it exists (set archived to true)
            original_highlights = self.notion_client.blocks.children.list(
                block_id=existing_page_id
            )
            if len(original_highlights["results"]):
                self.notion_client.pages.update(
                    page_id=original_highlights["results"][0]["id"], archived=True
                )
            # Create a new empty highlight page
            highlight_page_id = self.notion_client.pages.create(
                parent={"type": "page_id", "page_id": existing_page_id},
                properties={"title": [{"text": {"content": "Highlights"}}]},
            )
            return highlight_page_id["id"]
        else:
            page_ids = self.create_notion_page(book_title)
            logger.info(
                f"Created new pages for '{book_title}'. Main page ID: {page_ids['new_page_id']}, Highlight page ID: {page_ids['highlight_page_id']}"
            )
            return page_ids["highlight_page_id"]

    def sync_bookmarks(self):
        logger.info("Starting bookmark synchronization")
        book_titles = self.get_book_titles()
        for book_title in book_titles:
            # Get the highlight page ID
            highlight_page_id = self.get_or_create_page(book_title)
            # Get the highlights from the KoboReader.sqlite file
            bookmarks = self.load_bookmark(book_title, highlight_page_id)
            # Remove the leading and trailing whitespace (Source: https://github.com/starsdog/export_kobo)
            for j in range(0, len(bookmarks)):
                if bookmarks["Highlight"][j] != None:
                    bookmarks.loc[j, "Highlight"] = bookmarks["Highlight"][j].strip()
                    # Remove \n from the highlight
                    bookmarks.loc[j, "Highlight"] = bookmarks["Highlight"][j].replace(
                        "\n", ""
                    )
            # Write the highlights to the Notion page
            for x in range(0, len(bookmarks)):
                if bookmarks["Type"][x] == "highlight":
                    self.write_text(
                        highlight_page_id, bookmarks["Highlight"][x], "paragraph"
                    )
                else:
                    if bookmarks["Annotation"][x] != None:
                        self.write_text(
                            highlight_page_id, bookmarks["Annotation"][x], "quote"
                        )
                    if bookmarks["Highlight"][x] != None:
                        self.write_text(
                            highlight_page_id, bookmarks["Highlight"][x], "paragraph"
                        )

            logger.info(f"Synced {len(bookmarks)} bookmarks for '{book_title}'")

        logger.info("Bookmark synchronization completed")


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

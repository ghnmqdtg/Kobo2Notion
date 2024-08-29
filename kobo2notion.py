# Load packages
import os
import json
import dotenv
import logging
import sqlite3
import pandas as pd
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

    def load_bookmarks(self, title):
        raise NotImplementedError

    def check_page_exists(self, book_title, text):
        raise NotImplementedError

    def create_notion_page(self, titles):
        raise NotImplementedError

    def sync_bookmarks(self):
        # Step 1: Get the book titles
        book_titles = self.get_book_titles()
        # Step 2: Check if the book exists in Notion (if not, create it)
        raise NotImplementedError


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

# Load packages
import os
import dotenv
import logging
import sqlite3
import requests
import pandas as pd
from time import time
from notion_client import Client
from utils import CustomFormatter
import google.generativeai as genai

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
    def __init__(self, sqlite_path: str, notion_api_key: str, notion_db_id: str):
        self.connection = self._connect_to_sqlite(sqlite_path)
        self.notion_client = Client(auth=notion_api_key)
        self.notion_db_id = notion_db_id
        # Use Gemini API to summarize bookmarks
        if os.environ["SUMMARIZE_BOOKMARKS"] == "true":
            self.model = genai.GenerativeModel(os.environ["GEMINI_MODEL"])
            logger.info(f"Using {os.environ['GEMINI_MODEL']} to summarize bookmarks")
        else:
            self.model = None

    def _connect_to_sqlite(self, sqlite_path: str) -> sqlite3.Connection:
        """
        Connect to the SQLite database

        Args:
            sqlite_path (str): The path to the SQLite database

        Returns:
            sqlite3.Connection: The connection to the SQLite database
        """
        try:
            return sqlite3.connect(sqlite_path)
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite: {e}")
            return None

    def get_books_data(self) -> pd.DataFrame:
        """
        Load the book data from the SQLite database
        """
        # Query to get the book data from the SQLite database
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
        # logger.debug(f"Books data: {books_data}")
        logger.info(f"Retrieved data for {len(books_data)} books")
        return books_data

    def load_bookmark(self, title: str) -> pd.DataFrame:
        """
        Load the bookmark from the SQLite database

        Args:
            title (str): The title of the book

        Returns:
            pd.DataFrame: The bookmark data
        """
        # The content ID is used to get the bookmarks
        content_id = pd.read_sql(
            f"SELECT c.ContentId AS 'Content ID', c.Title AS 'Book Title' FROM content AS c WHERE c.Title LIKE '%{title}%'",
            self.connection,
        ).iloc[0]["Content ID"]
        # Load the bookmarks of the target book
        bookmark_df = pd.read_sql(
            f"SELECT VolumeID AS 'Volume ID', Text AS 'Highlight', Annotation, DateCreated AS 'Created On', Type FROM Bookmark Where VolumeID = '{content_id}' ORDER BY 4 ASC",
            self.connection,
        )
        # logger.debug(f"Loaded {len(bookmark_df)} bookmarks for '{title}'")
        return bookmark_df

    def fetch_book_cover(self, book_title: str, isbn: str) -> str:
        """
        Fetch the book cover from the Google Books API

        Args:
            book_title (str): The title of the book
            isbn (str): The ISBN of the book

        Returns:
            str: The URL of the book cover
        """
        # Search for the book cover using the book title
        response = requests.get(
            f"https://www.googleapis.com/books/v1/volumes?q={book_title}"
        )
        data = response.json()
        # Get the book ID from the Google Books API
        book_id = next(
            (
                item["id"]
                for item in data.get("items", [])
                if any(
                    identifier.get("type") == "ISBN_13"
                    and identifier.get("identifier") == isbn
                    for identifier in item.get("volumeInfo", {}).get(
                        "industryIdentifiers", []
                    )
                )
            ),
            data["items"][0]["id"] if data.get("items") else None,
        )
        # If the book ID is not found, return None
        if not book_id:
            logger.warning(f"Could not find book data for '{book_title}'")
            return None
        # Get the book cover image URL, we pass it to the Notion API to set the book cover
        image_url = f"https://books.google.com/books/publisher/content/images/frontcover/{book_id}?fife=w1200-h1200"
        return image_url if requests.get(image_url).status_code == 200 else None

    def get_or_create_page(self, book: pd.DataFrame) -> dict:
        """
        Get or create a page in Notion

        Args:
            book (pd.DataFrame): The book data

        Returns:
            dict: The page ID of the main page and highlight page
        """
        # Fetch the book cover image URL
        cover_url = self.fetch_book_cover(book["Book Title"], book["ISBN"])
        # Get the properties of the book
        properties = {
            "Title": {"title": [{"text": {"content": book["Book Title"]}}]},
            "Category": {"select": {"name": "Books"}},
            "Author": {"rich_text": [{"text": {"content": book["Author"]}}]},
            "Publisher": {"rich_text": [{"text": {"content": book["Publisher"]}}]},
            "ISBN": {"rich_text": [{"text": {"content": book["ISBN"]}}]},
            "Read Percent": {"number": book["Read Percent"]},
        }
        if book["Subtitle"]:
            properties["Subtitle"] = {
                "rich_text": [{"text": {"content": book["Subtitle"]}}]
            }
        # Query the existing page in Notion
        existing_page = self.notion_client.databases.query(
            database_id=self.notion_db_id,
            filter={"property": "Title", "title": {"equals": book["Book Title"]}},
        ).get("results", [])
        # If the page exists, update it
        if existing_page:
            return self._update_existing_page(
                existing_page[0]["id"], cover_url, properties
            )
        # If the page doesn't exist, create a new one
        else:
            return self._create_new_page(cover_url, properties)

    def _create_new_page(self, cover_url: str, properties: dict) -> dict:
        """
        Create a new page in Notion

        Args:
            cover_url (str): The URL of the book cover
            properties (dict): The properties of the book

        Returns:
            dict: The page ID of the main page and highlight page
        """
        main_page = self._create_main_page(cover_url, properties)
        highlight_page = self._create_highlight_page(main_page["id"])
        return {"parent_page": main_page["id"], "highlight_page": highlight_page["id"]}

    def _create_main_page(self, cover_url: str, properties: dict) -> dict:
        """
        Create a main page in Notion

        Args:
            cover_url (str): The URL of the book cover
            properties (dict): The properties of the book

        Returns:
            dict: The dictionary of the Notion page object
        """
        return self.notion_client.pages.create(
            parent={"database_id": self.notion_db_id},
            cover={"type": "external", "external": {"url": cover_url}},
            icon={"type": "external", "external": {"url": cover_url}},
            properties=properties,
        )

    def _create_highlight_page(self, parent_page_id: str) -> dict:
        """
        Create a highlight page under the parent page (which is the main page)

        Args:
            parent_page_id (str): The page ID of the main page

        Returns:
            dict: The dictionary of the Notion page object
        """
        return self.notion_client.pages.create(
            parent={"type": "page_id", "page_id": parent_page_id},
            properties={"title": [{"text": {"content": "Highlights"}}]},
        )

    def _update_existing_page(
        self, page_id: str, cover_url: str, properties: dict
    ) -> dict:
        """
        Update an existing page in Notion

        Args:
            page_id (str): The page ID of the main page
            cover_url (str): The URL of the book cover
            properties (dict): The properties of the book

        Returns:
            dict: The dictionary of the Notion page object
        """
        self.notion_client.pages.update(
            page_id=page_id,
            cover={"type": "external", "external": {"url": cover_url}},
            icon={"type": "external", "external": {"url": cover_url}},
            properties=properties,
        )
        # Delete old highlights and create a new one (Overwrite)
        self._archive_old_highlights(page_id)
        highlight_page = self._create_highlight_page(page_id)
        return {"parent_page": page_id, "highlight_page": highlight_page["id"]}

    def _archive_old_highlights(self, page_id: str):
        """
        Delete old highlights by archiving it

        Args:
            page_id (str): The page ID of the main page
        """
        # Get the original highlights
        original_highlights = self.notion_client.blocks.children.list(block_id=page_id)
        # If the original highlights exist, archive it
        if original_highlights["results"]:
            self.notion_client.pages.update(
                page_id=original_highlights["results"][0]["id"], archived=True
            )

    def sync_bookmarks(self):
        """
        Sync the bookmarks to Notion
        """
        logger.info("Starting bookmark synchronization")
        for _, book in self.get_books_data().iterrows():
            logger.info(f"{book['Book Title']} | Syncing bookmarks")
            start_time = time()
            # Get or create the main page and highlight page
            page_ids = self.get_or_create_page(book)
            # Load the bookmarks
            bookmarks = self.load_bookmark(book["Book Title"])

            # Sync original bookmarks
            bookmark_blocks = self._prepare_bookmark_blocks(bookmarks)
            self.sync_blocks(page_ids["highlight_page"], bookmark_blocks)
            logger.info(
                f"{book['Book Title']} | Synced {len(bookmark_blocks)} bookmarks in {time() - start_time:.2f} seconds"
            )

            # Summarize and sync summary if enabled
            if os.environ["SUMMARIZE_BOOKMARKS"] == "true":
                # Summarize bookmarks
                logger.info(f"{book['Book Title']} | Summarizing bookmarks")
                start_time = time()
                summary = self.summarize_bookmarks(book["Book Title"], bookmarks)
                logger.info(
                    f"{book['Book Title']} | Summarized bookmarks in {time() - start_time:.2f} seconds"
                )
                # Sync summary
                start_time = time()
                summary_blocks = parse_markdown_to_notion_blocks(summary)
                self.sync_blocks(page_ids["parent_page"], summary_blocks)
                logger.info(
                    f"{book['Book Title']} | Synced summary in {time() - start_time:.2f} seconds"
                )

        logger.info(f"All {len(self.get_books_data())} books are synced")

    def _prepare_bookmark_blocks(self, bookmarks: pd.DataFrame) -> list:
        """
        Prepare the bookmark blocks for Notion

        Args:
            bookmarks (pd.DataFrame): The bookmarks data of the book

        Returns:
            list: The list of bookmark blocks
        """
        blocks = []
        for _, bookmark in bookmarks.iterrows():
            content = ""
            if bookmark["Highlight"] is not None:
                content = str(bookmark["Highlight"]).strip().replace("\n", " ")

            block_type = "paragraph" if bookmark["Type"] == "highlight" else "quote"

            if bookmark["Type"] != "highlight" and bookmark["Annotation"]:
                annotation = str(bookmark["Annotation"]).strip()
                content = f"{annotation}\n{content}" if content else annotation

            if content:
                blocks.append(
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
        return blocks

    def sync_blocks(self, page_id: str, blocks: list):
        """
        Sync the blocks to the given page in Notion (limit 100 blocks per request)

        Args:
            page_id (str): The page ID of the main page
            blocks (list): The list of blocks to sync
        """
        for i in range(0, len(blocks), 100):
            batch = blocks[i : i + 100]
            try:
                self.notion_client.blocks.children.append(
                    block_id=page_id, children=batch
                )
            except Exception as e:
                logger.error(f"Error syncing blocks to Notion: {e}")

    def summarize_bookmarks(self, book_title: str, bookmarks: pd.DataFrame) -> str:
        """
        Summarize the bookmarks using Gemini API

        Args:
            book_title (str): The title of the book
            bookmarks (pd.DataFrame): The bookmarks data of the book

        Returns:
            str: The summary of the bookmarks
        """
        content = bookmarks["Highlight"].str.cat(sep="\n")
        # Use English prompt if the SUMMARIZE_LANGUAGE is en, otherwise use Traditional Chinese prompt
        if os.environ["SUMMARIZE_LANGUAGE"] == "en":
            prompt = f"""
            The following is a list of highlights from a book: {book_title}.
            ```
            {content}
            ```

            Please summarize the highlights into a concise and coherent summary using markdown format. Here are some guidelines:
            1. The highlights are ordered, but don't have a specific chapter or section, so please group them into sections.
            2. Please use bold text to highlight the most important words or sentences.
            3. It's okay to have numbers in the heading, such as "# 1. Section Title" or "# 二、段落標題"
            4. If there are duplicate highlights, please remove them to keep the summary concise.
            5. Please add abstract at the beginning and conclusion at the end.
            """
        else:
            prompt = f"""
            以下是從《{book_title}》節錄的重點：
            ```
            {content}
            ```
            請幫我以 markdown 格式統整、濃縮筆記，謝謝。以下為注意事項：
            1. 請以繁體中文回答。
            2. 這些重點的順序是連續的，但可能分散於不同章節，請依內容自行分類統整。謝謝。
            3. 直接回答重點，不要有任何額外的說明。
            4. 若有段落，其 heading 標籤可以同時附帶標號以更加醒目，例如：「# 一、段落標題」，其內容則以 numbered list 或 bullet point 表示。
            5. 冒號和括號以全形「：」和「（）」表示。
            6. 中、英文及數字間以半形空格隔開。
            7. 若重點有所重複，可以刪減以保持簡潔。
            8. 請於最開頭加上摘要，並於最後加上總結。
            """
        summary = self.model.generate_content(prompt)
        return summary.text


def parse_markdown_to_notion_blocks(markdown_text: str) -> list:
    """
    Parses Markdown text into a list of Notion blocks.
    Handles headings, paragraphs, lists, quotes, nested structures, and bold text.

    Args:
        markdown_text (str): The markdown text to parse

    Returns:
        list: The list of Notion blocks
    """
    notion_blocks = []
    lines = markdown_text.split("\n")
    current_list = None
    list_stack = []

    def create_block(block_type, content, children=None):
        """
        Create a block in Notion
        """
        block = {
            "object": "block",
            "type": block_type,
            block_type: {"rich_text": parse_rich_text(content)},
        }
        if children:
            block[block_type]["children"] = children
        return block

    def parse_rich_text(content):
        parts = content.split("**")
        rich_text = []
        for i, part in enumerate(parts):
            if part:
                text = {"type": "text", "text": {"content": part}}
                if i % 2 == 1:  # Odd indices are bold
                    text["annotations"] = {"bold": True}
                rich_text.append(text)
        return rich_text

    for line in lines:
        line = line.strip()
        if not line:
            continue

        indent = len(line) - len(line.lstrip())
        line = line.strip()

        if line.startswith("#"):
            # Heading
            level = min(len(line.split()[0]), 3)  # Cap at h3
            content = line.lstrip("#").strip()
            notion_blocks.append(create_block(f"heading_{level}", content))
            current_list = None
        elif line.startswith("- ") or line.startswith("* "):
            # Bulleted list item
            content = line[2:].strip()
            new_item = create_block("bulleted_list_item", content)

            if current_list and indent > list_stack[-1]:
                current_list["bulleted_list_item"]["children"].append(new_item)
            else:
                notion_blocks.append(new_item)
                current_list = new_item
                list_stack = [indent]
        elif line[0].isdigit() and ". " in line:
            # Numbered list item
            content = line.split(". ", 1)[1].strip()
            new_item = create_block("numbered_list_item", content)

            if current_list and indent > list_stack[-1]:
                current_list["numbered_list_item"]["children"].append(new_item)
            else:
                notion_blocks.append(new_item)
                current_list = new_item
                list_stack = [indent]
        elif line.startswith(">"):
            # Quote
            content = line[1:].strip()
            notion_blocks.append(create_block("quote", content))
            current_list = None
        else:
            # Paragraph
            notion_blocks.append(create_block("paragraph", line))
            current_list = None

    return notion_blocks


if __name__ == "__main__":
    # Check if the environment variables are loaded
    assert os.environ["NOTION_API_KEY"] is not None, "NOTION_API_KEY is not set"
    assert os.environ["NOTION_DB_ID"] is not None, "NOTION_DB_ID is not set"

    # Enable Gemini API if SUMMARIZE_BOOKMARKS is true
    if os.environ["SUMMARIZE_BOOKMARKS"] == "true":
        assert os.environ["GEMINI_API_KEY"] is not None, "GEMINI_API_KEY is not set"
        # Initialize Gemini client
        genai.configure(api_key=os.environ["GEMINI_API_KEY"])

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

    # logger.info("Initializing Kobo2Notion instance")
    kobo2notion = Kobo2Notion(
        sqlite_path="temp/KoboReader.sqlite",
        notion_api_key=os.environ["NOTION_API_KEY"],
        notion_db_id=os.environ["NOTION_DB_ID"],
    )
    kobo2notion.sync_bookmarks()
    logger.info("Kobo2Notion script completed")

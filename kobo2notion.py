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
    def __init__(self, sqlite_path, notion_api_key, notion_db_id):
        self.connection = self._connect_to_sqlite(sqlite_path)
        self.notion_client = Client(auth=notion_api_key)
        self.notion_db_id = notion_db_id
        logger.info("Kobo2Notion instance initialized")

    def _connect_to_sqlite(self, sqlite_path):
        try:
            return sqlite3.connect(sqlite_path)
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
        # logger.debug(f"Books data: {books_data}")
        logger.info(f"Retrieved data for {len(books_data)} books")
        return books_data

    def load_bookmark(self, title):
        content_id = pd.read_sql(
            f"SELECT c.ContentId AS 'Content ID', c.Title AS 'Book Title' FROM content AS c WHERE c.Title LIKE '%{title}%'",
            self.connection,
        ).iloc[0]["Content ID"]
        # Load the bookmark from the SQLite database
        bookmark_df = pd.read_sql(
            f"SELECT VolumeID AS 'Volume ID', Text AS 'Highlight', Annotation, DateCreated AS 'Created On', Type FROM Bookmark Where VolumeID = '{content_id}' ORDER BY 4 ASC",
            self.connection,
        )
        logger.debug(f"Loaded {len(bookmark_df)} bookmarks for '{title}'")
        return bookmark_df

    def fetch_book_cover(self, book_title, isbn):
        response = requests.get(
            f"https://www.googleapis.com/books/v1/volumes?q={book_title}"
        )
        data = response.json()
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

        if not book_id:
            logger.warning(f"Could not find book data for '{book_title}'")
            return None

        image_url = f"https://books.google.com/books/publisher/content/images/frontcover/{book_id}?fife=w1200-h1200"
        return image_url if requests.get(image_url).status_code == 200 else None

    def get_or_create_page(self, book):
        cover_url = self.fetch_book_cover(book["Book Title"], book["ISBN"])
        existing_page = self.notion_client.databases.query(
            database_id=self.notion_db_id,
            filter={"property": "Title", "title": {"equals": book["Book Title"]}},
        ).get("results", [])

        if existing_page:
            return self._update_existing_page(existing_page[0]["id"], cover_url)
        else:
            return self._create_new_pages(book, cover_url)

    def _create_new_pages(self, book, cover_url):
        main_page = self._create_main_page(book, cover_url)
        highlight_page = self._create_highlight_page(main_page["id"])
        return {"parent_page": main_page["id"], "highlight_page": highlight_page["id"]}

    def _create_main_page(self, book, cover_url):
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

        return self.notion_client.pages.create(
            parent={"database_id": self.notion_db_id},
            cover={"type": "external", "external": {"url": cover_url}},
            icon={"type": "external", "external": {"url": cover_url}},
            properties=properties,
        )

    def _create_highlight_page(self, parent_page_id):
        return self.notion_client.pages.create(
            parent={"type": "page_id", "page_id": parent_page_id},
            properties={"title": [{"text": {"content": "Highlights"}}]},
        )

    def _update_existing_page(self, page_id, cover_url):
        self.notion_client.pages.update(
            page_id=page_id,
            cover={"type": "external", "external": {"url": cover_url}},
            icon={"type": "external", "external": {"url": cover_url}},
        )
        self._archive_old_highlights(page_id)
        highlight_page = self._create_highlight_page(page_id)
        return {"parent_page": page_id, "highlight_page": highlight_page["id"]}

    def _archive_old_highlights(self, page_id):
        original_highlights = self.notion_client.blocks.children.list(block_id=page_id)
        if original_highlights["results"]:
            self.notion_client.pages.update(
                page_id=original_highlights["results"][0]["id"], archived=True
            )

    def sync_bookmarks(self):
        logger.info("Starting bookmark synchronization")
        for _, book in self.get_books_data().iterrows():
            page_ids = self.get_or_create_page(book)
            bookmarks = self.load_bookmark(book["Book Title"])

            if os.environ["SUMMARIZE_BOOKMARKS"] == "true":
                summary = self.summarize_bookmarks(book["Book Title"], bookmarks)
                summary_blocks = self.parse_markdown_to_notion_blocks(summary)
                for i in range(0, len(summary_blocks), 100):
                    self.notion_client.blocks.children.append(
                        block_id=page_ids["parent_page"],
                        children=summary_blocks[i : i + 100],
                    )

            self._sync_highlights(page_ids["highlight_page"], bookmarks)
        logger.info("Bookmark synchronization completed")

    def _sync_highlights(self, highlight_page, bookmarks):
        children_blocks = []
        for _, bookmark in bookmarks.iterrows():
            if bookmark["Highlight"] is not None:
                content = str(bookmark["Highlight"]).strip().replace("\n", " ")
            block_type = "paragraph" if bookmark["Type"] == "highlight" else "quote"
            if bookmark["Type"] != "highlight" and bookmark["Annotation"]:
                content = f"{bookmark['Annotation']}\n{content}"

            children_blocks.append(
                {
                    "object": "block",
                    "type": block_type,
                    block_type: {
                        "rich_text": [{"type": "text", "text": {"content": content}}]
                    },
                }
            )

            if len(children_blocks) == 100:
                self._send_bookmark_batch(highlight_page, children_blocks)
                children_blocks = []

        if children_blocks:
            self._send_bookmark_batch(highlight_page, children_blocks)

    def _send_bookmark_batch(self, highlight_page, children_blocks):
        try:
            self.notion_client.blocks.children.append(
                block_id=highlight_page, children=children_blocks
            )
        except Exception as e:
            logger.error(f"Error syncing bookmarks to Notion: {e}")

    def summarize_bookmarks(self, book_title, bookmarks):
        logger.info(f"Summarizing bookmarks for '{book_title}'")
        # Use Gemini API to summarize bookmarks
        model = genai.GenerativeModel(os.environ["GEMINI_MODEL"])
        content = bookmarks["Highlight"].str.cat(sep="\n")
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
            請幫我以 markdown 格式統整，並以繁體中文回答，以下為注意事項：
            1. 請以繁體中文回答。
            2. 這些重點的順序是連續的，但可能分散於不同章節，請依內容自行分類統整。謝謝。
            3. 直接回答重點，不要有任何額外的說明。
            4. 若有段落，其 heading 標籤可以同時附帶標號以更加醒目，例如：「# 一、段落標題」，其內容則以 numbered list 或 bullet point 表示。
            5. 冒號和括號以全形「：」和「（）」表示。
            6. 中、英文及數字間以半形空格隔開。
            7. 若重點有所重複，可以刪減以保持簡潔。
            8. 請於最開頭加上摘要，並於最後加上總結。
            """
        summary = model.generate_content(prompt)
        return summary.text

    def parse_markdown_to_notion_blocks(self, markdown_text):
        """
        Parses Markdown text into a list of Notion blocks.
        Handles headings, paragraphs, lists, quotes, nested structures, and bold text.
        """
        notion_blocks = []
        lines = markdown_text.split("\n")
        current_list = None
        list_stack = []

        def create_block(block_type, content, children=None):
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

    logger.info("Initializing Kobo2Notion instance")
    kobo2notion = Kobo2Notion(
        sqlite_path="temp/KoboReader.sqlite",
        notion_api_key=os.environ["NOTION_API_KEY"],
        notion_db_id=os.environ["NOTION_DB_ID"],
    )
    kobo2notion.sync_bookmarks()
    logger.info("Kobo2Notion script completed")

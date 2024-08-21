# Load packages
import os
import dotenv
import pandas as pd
from notion_client import Client

# Load environment variables
dotenv.load_dotenv()


if __name__ == "__main__":
    # Check if the environment variables are loaded
    assert os.environ["NOTION_API_KEY"] is not None, "NOTION_API_KEY is not set"
    assert os.environ["NOTION_DB_ID"] is not None, "NOTION_DB_ID is not set"

    # Initialize the Notion client
    notion = Client(auth=os.environ["NOTION_API_KEY"])
    # Get the database
    database = notion.databases.retrieve(database_id=os.environ["NOTION_DB_ID"])
    print(database)

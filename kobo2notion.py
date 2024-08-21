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

    # Initialize the Notion client
    notion = Client(auth=os.environ["NOTION_API_KEY"])
    # Get the database
    database = notion.databases.retrieve(database_id=os.environ["NOTION_DB_ID"])
    print(database)

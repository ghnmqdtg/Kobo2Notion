# Kobo2Notion

## Introduction
Kobo2Notion is an Electron application built with TypeScript and React that extracts bookmark data from Kobo e-readers and seamlessly uploads it into your Notion database. Additionally, it can summarize bookmarks using Google Gemini. This project offers a cost-free alternative to existing solutions like Readwise, providing users with greater control over their reading data.

> This app is still under development. Some features are not implemented yet.

<img src="assets/demo_01.png" width="80%">

## Features
- Extract bookmarks from Kobo e-readers
- Upload bookmarks to a Notion database
- Summarize bookmarks using Google Gemini AI
- Modern desktop interface built with Electron
- Free and open-source

## Prerequisites
- Node.js 18 or later
- Kobo e-reader
- Notion account
- Google Cloud account (for Gemini API access)

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/ghnmqdtg/Kobo2Notion.git
   cd Kobo2Notion
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Create your environment configuration:
   ```bash
   cp .env.example .env
   ```

   Example `.env` file:
   ```env
   # SQLite Source (The default path is defined in MacOS)
   SQLITE_SOURCE=/Volumes/KOBOeReader/.kobo/KoboReader.sqlite

   # Notion API Key and Database ID
   NOTION_API=your_notion_api_key_here
   NOTION_DB=your_notion_database_id_here

   # Summarize Bookmarks
   SUMMARIZE_ENABLED=true
   SUMMARIZE_LANGUAGE=zh
   GEMINI_MODEL=gemini-1.5-flash
   GEMINI_API=your_gemini_api_key_here
   ```

## Configuration
1. Connect your Kobo e-reader to your computer.

2. Set the `SQLITE_SOURCE` in your `.env` file to the path of the KoboReader.sqlite file:
   - MacOS: `/Volumes/KOBOeReader/.kobo/KoboReader.sqlite`
   - Windows: Usually under the drive letter assigned to your Kobo device, e.g., `E:\.kobo\KoboReader.sqlite`

3. Get the Notion API key

    1. Go to [Notion Integrations](https://www.notion.so/profile/integrations)

    2. Create a new integration named `kobo-export` and set associated workspace to your workspace.

    3. Click `Save`.

        <p align="left">
            <img src="assets/notion_integration_01.png" width="70%">
        </p>

    4. Click `Show` and copy the `SECRET` value.
        
        <p align="left">
            <img src="assets/notion_integration_02.png" width="70%">
        </p>
    
    5. Paste the `SECRET` value into the `NOTION_API` environment variable in `.env` file.

4. Get the Notion Database ID

    1. Duplicate the database template [here](https://ghnmqdtg.notion.site/4978bcc5eda847a59940f5cb4aff32d9?v=28a249bcfa92488889f3505127a8e1ef&pvs=4) to your workspace.

        <p align="left">
            <img src="assets/notion_database_01.png" width="70%">
        </p>

    2. Click `Share` and copy the link.

        <p align="left">
            <img src="assets/notion_database_02.png" height="300pt">
        </p>
    
    3. Extract the `Notion Database ID` from the URL.

        For example, if the URL is `https://www.notion.so/ghnmqdtg/4978bcc5eda847a59940f5cb4aff32d9?v=28a249bcfa92488889f3505127a8e1ef&pvs=4`, the `Notion Database ID` is `4978bcc5eda847a59940f5cb4aff32d9`.

    4. Paste the `Notion Database ID` value into the `NOTION_DB` environment variable in `.env` file.

    5. Connect the database to the `kobo-export` integration.

        <p align="left">
            <img src="assets/notion_database_03.png" height="300pt">
        </p>

5. Get the Google Gemini API key [here](https://aistudio.google.com/app/apikey) (optional)

    > This is an optional feature if you want to summarize your bookmarks. The API for `gemini-1.5-flash` is free on Google AI Studio. If you don't need it, set `SUMMARIZE_ENABLED` to `false` in `.env` file.

    Copy the `API Key` value into the `GEMINI_API` environment variable in `.env` file.

    <p align="left">
        <img src="assets/gemini_key_01.png" width="70%">
    </p>

## Usage
1. Start the application in development mode:
   ```bash
   npm run dev
   ```

   Or build and run the production version:
   ```bash
   npm run build
   npm start
   ```

2. The application will display your Kobo library. Select the books you want to export and click the export button.

3. Check your Notion database to see the exported bookmarks and summaries.

    1. The overview of the library. You can see all the books you purchased.

        <p align="left">
            <img src="assets/demo_01.png" width="70%">
        </p>

    2. Once you select and export a book, the application will start to extract the bookmarks and upload them to Notion.
    
        <p align="left">
            <img src="assets/demo_02.png" width="70%">
        </p>
    
    3. The detail of the book. The original highlight is saved in `Highlight` page, and the summary is saved in the main page.
        
        <p align="left">
            <img src="assets/demo_03.png" width="70%">
        </p>

## Building for Distribution
To build the application for your platform:

```bash
# For Windows
npm run build:win

# For macOS
npm run build:mac

# For Linux
npm run build:linux
```

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## License
This project is licensed under the [MIT License](LICENSE).

## Acknowledgements
- [mollykannn/kobo2notion](https://github.com/mollykannn/kobo2notion)
- [starsdog/export_kobo](https://github.com/starsdog/export_kobo)
- [Kobo bookmark](https://github.com/huybn5776/kobo-bookmark)
- Notion for their API
- Google for the Gemini AI model

## Contact
For questions or support, please open an issue on the GitHub repository.
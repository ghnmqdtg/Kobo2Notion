<!-- <img align="center" src="resources/icon.png" height="32pt"> Kobo2Notion -->

<p align="center">
    <img src="assets/badge.png" width="80%">
</p>

---
<br>

Kobo2Notion is an Electron application built with TypeScript and React that extracts bookmark data from Kobo e-readers and seamlessly uploads it into your Notion database. Additionally, it can summarize bookmarks using Google Gemini. This project offers a cost-free alternative to existing solutions like Readwise, providing users with greater control over their reading data.

<p align="center">
    <img src="assets/demo_01.png" width="90%">
</p>

<!-- Table of Contents -->
## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#how-can-i-get-the-values-for-the-configuration)
- [Building for Distribution](#building-for-distribution)
- [Contributing](#contributing)
- [Contact](#contact)
- [License](#license)
- [Acknowledgements](#acknowledgements)
- [Buy me a coffee](#buy-me-a-coffee)

## Features
- Extract bookmarks from Kobo e-readers
- Upload bookmarks to a Notion database
- Summarize bookmarks using Google Gemini AI
- Modern desktop interface built with Electron
- Free and open-source

## Prerequisites
- Node.js 18 or later (tested on v22.2.0)
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

2. When you first launch the app, you'll be prompted to enter your configuration in the Settings page:
   - Kobo Highlights File Path (e.g., `/Volumes/KOBOeReader/.kobo/KoboReader.sqlite` on MacOS)
   - Notion API Key
   - Notion Database ID
   - Gemini API Key (optional, for bookmark summarization)
   > How can I get these values? Please refer to the [Config](#how-can-i-get-the-values-for-the-configuration) section.


3. After saving your settings, the application will display your Kobo library. Select the books you want to export and click the export button.

4. Check your Notion database to see the exported bookmarks and summaries.

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

## How can I get the values for the configuration?

<details>
<summary>Click to read the details</summary>

1. The path of the `KoboReader.sqlite` file

   - MacOS: `/Volumes/KOBOeReader/.kobo/KoboReader.sqlite`
   - Windows: Usually under the drive letter assigned to your Kobo device, e.g., `E:\.kobo\KoboReader.sqlite`

2. Notion API key

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

3. Notion Database ID

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

4. Connect the your database to the `kobo-export` integration.

    <p align="left">
        <img src="assets/notion_database_03.png" height="300pt">
    </p>

5. Google Gemini API key [here](https://aistudio.google.com/app/apikey) (optional)

    > This is an optional feature if you want to summarize your bookmarks. The API for `gemini-1.5-flash` is free on Google AI Studio.

    <p align="left">
        <img src="assets/gemini_key_01.png" width="70%">
    </p>

</details>

## Building for Distribution
You can build the app by running:
```bash
# Build for MacOS
npm run build:mac

# Build for Windows
npm run build:win

# Build for Linux
npm run build:linux
```

Windows and Linux are not yet supported. I've only built the app for MacOS. However, I haven't signed the app yet because my Apple Developer Program membership has expired, and I don't have a budget to renew it. Therefore, I cannot distribute the app on the macOS. If you want to use the app, you can build it yourself, or just run the app in development mode.

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## Contact
For questions or support, please open an issue on the GitHub repository.

## License
This project is licensed under the [MIT License](LICENSE).


## Acknowledgements
- [mollykannn/kobo2notion](https://github.com/mollykannn/kobo2notion)
- [starsdog/export_kobo](https://github.com/starsdog/export_kobo)
- [huybn5776/Kobo bookmark](https://github.com/huybn5776/kobo-bookmark) (for using corsproxy to avoid CORS issues)
- Notion for their API
- Google for the Gemini AI model

## Buy me a coffee
If you like this app, please consider buying me a coffee and save this poor guy ;(
<p align="left">
    <a href="https://www.buymeacoffee.com/tweizh" target="_blank"><img src="https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png" alt="Buy Me A Coffee" style="height: 41px !important;width: 174px !important;box-shadow: 0px 3px 2px 0px rgba(190, 190, 190, 0.5) !important;-webkit-box-shadow: 0px 3px 2px 0px rgba(190, 190, 190, 0.5) !important;" ></a>
</p>
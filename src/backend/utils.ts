import { Block, NotionBlock, RichTextItem } from './models';


/**
 * Normalizes a string by converting it to lowercase and removing punctuation and special characters.
 * @param str - The string to normalize.
 * @returns The normalized string.
 */
function normalizeString(str: string): string {
  return str
    .toLowerCase() // Convert to lowercase
    .replace(/[‧•]/g, '') // Remove specific middle dot variations
    .replace(/[^\w\s]/g, '') // Remove punctuation and special characters
    .replace(/─/g, '') // Remove ─
    .replace(/[:：]/g, ''); // Remove full width and half width colon
}

/**
 * Fetches the book cover from Google Books.
 * @param bookTitle - The title of the book.
 * @param isbn - The ISBN of the book.
 * @returns The URL of the book cover.
 */
export async function fetchBookCover(bookTitle: string, isbn: string): Promise<string> {
    const response = await fetch(`https://www.googleapis.com/books/v1/volumes?q=${bookTitle}`);
    const data = await response.json();

    const bookId = data.items?.find((item: any) =>
        item.volumeInfo?.industryIdentifiers?.some(
            (id: any) => id.type === 'ISBN_13' && id.identifier === isbn
        )
    )?.id ?? data.items?.find((item: any) => {
        console.log(normalizeString(item.volumeInfo?.title), normalizeString(bookTitle));
        return normalizeString(item.volumeInfo?.title) === normalizeString(bookTitle);
    })?.id;

    if (!bookId) {
        console.warn(`Could not find book data for '${bookTitle}'`);
        return ''; // Return empty string instead of null
    }

    const imageUrl = `https://books.google.com/books/publisher/content/images/frontcover/${bookId}?fife=w1200-h1200`;

    return imageUrl;
}

/**
 * Parses a markdown text into a list of Notion blocks.
 * @param markdownText - The markdown text to parse.
 * @returns A list of Notion blocks.
 */
export function parseMarkdownToNotionBlocks(markdownText: string): NotionBlock[] {
    const notionBlocks: NotionBlock[] = [];
    const lines = markdownText.split('\n');
    let currentList: any = null;
    const listStack: number[] = [];

    function createBlock(
        blockType: 'paragraph' | 'heading_1' | 'heading_2' | 'heading_3' | 'bulleted_list_item' | 'numbered_list_item' | 'quote',
        content: string,
        children: NotionBlock[] = []
    ): NotionBlock {
        const block: NotionBlock = {
            object: 'block',
            type: blockType,
            [blockType]: {
                rich_text: parseRichText(content),
            },
        } as NotionBlock;

        if (children.length > 0) {
            (block[blockType] as any).children = children;
        }
        return block;
    }

    function parseRichText(content: string): RichTextItem[] {
        const parts = content.split('**');
        const richText: RichTextItem[] = [];
        for (let i = 0; i < parts.length; i++) {
            if (parts[i]) {
                const text: RichTextItem = {
                    type: 'text',
                    text: { content: parts[i] },
                };
                if (i % 2 === 1) {
                    text.annotations = { bold: true };
                }
                richText.push(text);
            }
        }
        return richText;
    }

    for (const line of lines) {
        const trimmedLine = line.trim();
        if (!trimmedLine) {
            currentList = null;
            listStack.length = 0;
            continue;
        }

        const indent = line.length - line.trimStart().length;

        if (trimmedLine.startsWith('#')) {
            // Heading
            const level = Math.min(trimmedLine.split(' ')[0].length, 3);
            const content = trimmedLine.substring(level).trim();
            const heading_level = `heading_${level}` as 'heading_1' | 'heading_2' | 'heading_3';
            notionBlocks.push(createBlock(heading_level, content));
            currentList = null;
            listStack.length = 0;
        } else if (trimmedLine.startsWith('- ') || trimmedLine.startsWith('* ')) {
            // Bulleted list item
            const content = trimmedLine.substring(2).trim();
            const newListItem = createBlock('bulleted_list_item', content);

            if (currentList && currentList.type === 'bulleted_list_item' && indent > listStack[listStack.length - 1]) {
                currentList.bulleted_list_item.children = currentList.bulleted_list_item.children || [];
                currentList.bulleted_list_item.children.push(newListItem);
            } else {
                notionBlocks.push(newListItem);
                currentList = newListItem;
                listStack.push(indent);
            }
        } else if (/^\d+\.\s/.test(trimmedLine)) {
            // Numbered list item
            const content = trimmedLine.substring(trimmedLine.indexOf('.') + 2).trim();
            const newListItem = createBlock('numbered_list_item', content);

            if (currentList && currentList.type === 'numbered_list_item' && indent > listStack[listStack.length - 1]) {
                currentList.numbered_list_item.children = currentList.numbered_list_item.children || [];
                currentList.numbered_list_item.children.push(newListItem);
            } else {
                notionBlocks.push(newListItem);
                currentList = newListItem;
                listStack.push(indent);
            }
        } else if (trimmedLine.startsWith('>')) {
            // Quote
            const content = trimmedLine.substring(1).trim();
            notionBlocks.push(createBlock('quote', content));
            currentList = null;
            listStack.length = 0;
        } else {
            // Paragraph
            notionBlocks.push(createBlock('paragraph', trimmedLine));
            currentList = null;
            listStack.length = 0;
        }
    }

    return notionBlocks;
}
import { BlockObjectRequest, RichTextItemRequest } from '@notionhq/client/build/src/api-endpoints';

export async function fetchBookCover(bookTitle: string, isbn: string): Promise<string> {
    const response = await fetch(`https://www.googleapis.com/books/v1/volumes?q=${bookTitle}`);
    const data = await response.json();

    const bookId = data.items?.find((item: any) =>
        item.volumeInfo?.industryIdentifiers?.some(
            (id: any) => id.type === 'ISBN_13' && id.identifier === isbn
        )
    )?.id ?? data.items?.[0]?.id;

    if (!bookId) {
        console.warn(`Could not find book data for '${bookTitle}'`);
        return ''; // Return empty string instead of null
    }

    const imageUrl = `https://books.google.com/books/publisher/content/images/frontcover/${bookId}?fife=w1200-h1200`;
    const imageResponse = await fetch(imageUrl);
    return imageResponse.status === 200 ? imageUrl : ''; // Return empty string if not found
}

export function parseMarkdownToNotionBlocks(markdownText: string): BlockObjectRequest[] {
    const notionBlocks: BlockObjectRequest[] = [];
    const lines = markdownText.split('\n');
    let currentList: any = null;
    const listStack: number[] = [];

    function createBlock(blockType: string, content: string, children: BlockObjectRequest[] = []): BlockObjectRequest {
        const block: BlockObjectRequest = {
            object: 'block',
            type: blockType,
            [blockType]: {
                rich_text: parseRichText(content),
            },
        };
        if (children.length > 0) {
            block[blockType] = {
                ...block[blockType],
                children: children,
            };
        }
        return block;
    }

    function parseRichText(content: string): RichTextItemRequest[] {
        const parts = content.split('**');
        const richText: RichTextItemRequest[] = [];
        for (let i = 0; i < parts.length; i++) {
            if (parts[i]) {
                const text: RichTextItemRequest = {
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
            notionBlocks.push(createBlock(`heading_${level}`, content));
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
import { Block, NotionBlock, RichTextItem } from "./models";

/**
 * Fetches the book cover from Kobo CDN.
 * @param imageId - The image ID of the book.
 * @returns The URL of the book cover.
 */
export async function fetchBookCover(imageId: string): Promise<string> {
  // Get the image URL from Kobo CDN, using corsproxy to avoid CORS issues
  return `https://corsproxy.io/?url=https://cdn.kobo.com/book-images/${imageId}/800/800/90/False/0.jpg`;
}

/**
 * Parses a markdown text into a list of Notion blocks.
 * @param markdownText - The markdown text to parse.
 * @returns A list of Notion blocks.
 */
export function parseMarkdownToNotionBlocks(
  markdownText: string,
): NotionBlock[] {
  const notionBlocks: NotionBlock[] = [];
  const lines = markdownText.split("\n");
  let currentList: NotionBlock | null = null;
  const listStack: number[] = [];

  function createBlock(
    blockType:
      | "paragraph"
      | "heading_1"
      | "heading_2"
      | "heading_3"
      | "bulleted_list_item"
      | "numbered_list_item"
      | "quote",
    content: string,
    children: NotionBlock[] = [],
  ): NotionBlock {
    const block: NotionBlock = {
      object: "block",
      type: blockType,
      [blockType]: {
        rich_text: parseRichText(content),
      },
    } as NotionBlock;

    if (children.length > 0) {
      (
        block[blockType] as { rich_text: RichTextItem[]; children?: Block[] }
      ).children = children;
    }
    return block;
  }

  function parseRichText(content: string): RichTextItem[] {
    const parts = content.split("**");
    const richText: RichTextItem[] = [];
    for (let i = 0; i < parts.length; i++) {
      if (parts[i]) {
        const text: RichTextItem = {
          type: "text",
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

    if (trimmedLine.startsWith("#")) {
      // Heading
      const level = Math.min(trimmedLine.split(" ")[0].length, 3);
      const content = trimmedLine.substring(level).trim();
      const heading_level = `heading_${level}` as
        | "heading_1"
        | "heading_2"
        | "heading_3";
      notionBlocks.push(createBlock(heading_level, content));
      currentList = null;
      listStack.length = 0;
    } else if (trimmedLine.startsWith("- ") || trimmedLine.startsWith("* ")) {
      // Bulleted list item
      const content = trimmedLine.substring(2).trim();
      const newListItem = createBlock("bulleted_list_item", content);

      if (
        currentList &&
        currentList.type === "bulleted_list_item" &&
        indent > listStack[listStack.length - 1]
      ) {
        currentList.bulleted_list_item.children =
          currentList.bulleted_list_item.children || [];
        currentList.bulleted_list_item.children.push(newListItem);
      } else {
        notionBlocks.push(newListItem);
        currentList = newListItem;
        listStack.push(indent);
      }
    } else if (/^\d+\.\s/.test(trimmedLine)) {
      // Numbered list item
      const content = trimmedLine
        .substring(trimmedLine.indexOf(".") + 2)
        .trim();
      const newListItem = createBlock("numbered_list_item", content);

      if (
        currentList &&
        currentList.type === "numbered_list_item" &&
        indent > listStack[listStack.length - 1]
      ) {
        currentList.numbered_list_item.children =
          currentList.numbered_list_item.children || [];
        currentList.numbered_list_item.children.push(newListItem);
      } else {
        notionBlocks.push(newListItem);
        currentList = newListItem;
        listStack.push(indent);
      }
    } else if (trimmedLine.startsWith(">")) {
      // Quote
      const content = trimmedLine.substring(1).trim();
      notionBlocks.push(createBlock("quote", content));
      currentList = null;
      listStack.length = 0;
    } else {
      // Paragraph
      notionBlocks.push(createBlock("paragraph", trimmedLine));
      currentList = null;
      listStack.length = 0;
    }
  }

  return notionBlocks;
}

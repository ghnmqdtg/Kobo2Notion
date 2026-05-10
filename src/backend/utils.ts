/// <reference types="node" />
import { Block, NotionBlock, RichTextItem } from './models'

const KOBO_CDN_BASE = 'https://cdn.kobo.com/book-images'
const CORS_PROXY = 'https://api.allorigins.win/raw?url='

/**
 * Returns a proxied Kobo CDN URL for a book cover. Used by the Notion API,
 * which cannot access Kobo CDN directly.
 */
export async function fetchBookCover(imageId: string): Promise<string> {
  if (!imageId) return ''
  const koboUrl = `${KOBO_CDN_BASE}/${imageId}/800/800/90/False/0.jpg`
  return `${CORS_PROXY}${encodeURIComponent(koboUrl)}`
}

/**
 * Fetches the book cover from Kobo CDN in Node.js context (no CORS) and returns
 * a base64 data URL suitable for use as an <img src> in the renderer.
 */
export async function fetchBookCoverDataUrl(imageId: string): Promise<string> {
  if (!imageId) return ''
  const koboUrl = `${KOBO_CDN_BASE}/${imageId}/800/800/90/False/0.jpg`
  const url = `${CORS_PROXY}${encodeURIComponent(koboUrl)}`
  const response = await fetch(url)
  if (!response.ok) throw new Error(`Failed to fetch cover: ${response.status}`)
  const buffer = await response.arrayBuffer()
  const base64 = Buffer.from(buffer).toString('base64')
  const contentType = response.headers.get('content-type') || 'image/jpeg'
  return `data:${contentType};base64,${base64}`
}

/**
 * Parses a markdown text into a list of Notion blocks.
 * @param markdownText - The markdown text to parse.
 * @returns A list of Notion blocks.
 */
export function parseMarkdownToNotionBlocks(markdownText: string): NotionBlock[] {
  const notionBlocks: NotionBlock[] = []
  const lines = markdownText.split('\n')
  let currentList: NotionBlock | null = null
  const listStack: number[] = []

  function createBlock(
    blockType:
      | 'paragraph'
      | 'heading_1'
      | 'heading_2'
      | 'heading_3'
      | 'bulleted_list_item'
      | 'numbered_list_item'
      | 'quote',
    content: string,
    children: NotionBlock[] = []
  ): NotionBlock {
    const block: NotionBlock = {
      object: 'block',
      type: blockType,
      [blockType]: {
        rich_text: parseRichText(content)
      }
    } as NotionBlock

    if (children.length > 0) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ;(block as any)[blockType].children = children
    }
    return block
  }

  function parseRichText(content: string): RichTextItem[] {
    const parts = content.split('**')
    const richText: RichTextItem[] = []
    for (let i = 0; i < parts.length; i++) {
      if (parts[i]) {
        const text: RichTextItem = {
          type: 'text',
          text: { content: parts[i] }
        }
        if (i % 2 === 1) {
          text.annotations = { bold: true }
        }
        richText.push(text)
      }
    }
    return richText
  }

  for (const line of lines) {
    const trimmedLine = line.trim()
    if (!trimmedLine) {
      currentList = null
      listStack.length = 0
      continue
    }

    const indent = line.length - line.trimStart().length

    if (trimmedLine.startsWith('#')) {
      // Heading
      const level = Math.min(trimmedLine.split(' ')[0].length, 3)
      const content = trimmedLine.substring(level).trim()
      const heading_level = `heading_${level}` as 'heading_1' | 'heading_2' | 'heading_3'
      notionBlocks.push(createBlock(heading_level, content))
      currentList = null
      listStack.length = 0
    } else if (trimmedLine.startsWith('- ') || trimmedLine.startsWith('* ')) {
      // Bulleted list item
      const content = trimmedLine.substring(2).trim()
      const newListItem = createBlock('bulleted_list_item', content)

      if (
        currentList &&
        currentList.type === 'bulleted_list_item' &&
        indent > listStack[listStack.length - 1]
      ) {
        currentList.bulleted_list_item.children = currentList.bulleted_list_item.children || []
        currentList.bulleted_list_item.children.push(newListItem)
      } else {
        notionBlocks.push(newListItem)
        currentList = newListItem
        listStack.push(indent)
      }
    } else if (/^\d+\.\s/.test(trimmedLine)) {
      // Numbered list item
      const content = trimmedLine.substring(trimmedLine.indexOf('.') + 2).trim()
      const newListItem = createBlock('numbered_list_item', content)

      if (
        currentList &&
        currentList.type === 'numbered_list_item' &&
        indent > listStack[listStack.length - 1]
      ) {
        currentList.numbered_list_item.children = currentList.numbered_list_item.children || []
        currentList.numbered_list_item.children.push(newListItem)
      } else {
        notionBlocks.push(newListItem)
        currentList = newListItem
        listStack.push(indent)
      }
    } else if (trimmedLine.startsWith('>')) {
      // Quote
      const content = trimmedLine.substring(1).trim()
      notionBlocks.push(createBlock('quote', content))
      currentList = null
      listStack.length = 0
    } else {
      // Paragraph
      notionBlocks.push(createBlock('paragraph', trimmedLine))
      currentList = null
      listStack.length = 0
    }
  }

  return notionBlocks
}

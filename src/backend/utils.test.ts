/* eslint-disable @typescript-eslint/ban-ts-comment, @typescript-eslint/no-namespace */
// @ts-nocheck — Test file; Jest globals and discriminated union property access bypass type-checking
import { fetchBookCover, parseMarkdownToNotionBlocks } from './utils'

describe('fetchBookCover', () => {
  it('should return a plain corsproxy URL when no key is set', async () => {
    const imageId = 'abc-123-def'
    const result = await fetchBookCover(imageId)
    // CORSPROXY_KEY is empty in test env, so no key param
    expect(result).toStartWith('https://corsproxy.io/?url=')
    expect(result).toContain('cdn.kobo.com')
    expect(result).not.toContain('key=')
  })

  it('should proxy image IDs with special characters', async () => {
    const imageId = 'some/path/with-dashes'
    const result = await fetchBookCover(imageId)
    expect(result).toStartWith('https://corsproxy.io/?url=')
    expect(result).toContain('cdn.kobo.com')
  })

  it('should return empty string for empty image ID', async () => {
    const result = await fetchBookCover('')
    expect(result).toBe('')
  })
})

expect.extend({
  toStartWith(received: string, expected: string) {
    const pass = received.startsWith(expected)
    return {
      message: () => `expected ${received} to start with ${expected}`,
      pass
    }
  }
})

declare global {
  namespace jest {
    interface Matchers<R> {
      toStartWith(expected: string): R
    }
  }
}

describe('parseMarkdownToNotionBlocks', () => {
  describe('headings', () => {
    it('should parse h1 heading', () => {
      const blocks = parseMarkdownToNotionBlocks('# Hello World')
      expect(blocks).toHaveLength(1)
      expect(blocks[0].type).toBe('heading_1')
      expect(blocks[0].heading_1!.rich_text[0].text.content).toBe('Hello World')
    })

    it('should parse h2 heading', () => {
      const blocks = parseMarkdownToNotionBlocks('## Section Title')
      expect(blocks).toHaveLength(1)
      expect(blocks[0].type).toBe('heading_2')
      expect(blocks[0].heading_2!.rich_text[0].text.content).toBe('Section Title')
    })

    it('should parse h3 heading', () => {
      const blocks = parseMarkdownToNotionBlocks('### Subsection')
      expect(blocks).toHaveLength(1)
      expect(blocks[0].type).toBe('heading_3')
      expect(blocks[0].heading_3!.rich_text[0].text.content).toBe('Subsection')
    })

    it('should clamp headings deeper than h3 to h3', () => {
      const blocks = parseMarkdownToNotionBlocks('#### Deep heading')
      expect(blocks).toHaveLength(1)
      // #### has 4 hashes but Math.min(..., 3) clamps to 3
      expect(blocks[0].type).toBe('heading_3')
    })
  })

  describe('paragraphs', () => {
    it('should parse plain text as paragraph', () => {
      const blocks = parseMarkdownToNotionBlocks('Just some text.')
      expect(blocks).toHaveLength(1)
      expect(blocks[0].type).toBe('paragraph')
      expect(blocks[0].paragraph!.rich_text[0].text.content).toBe('Just some text.')
    })

    it('should skip empty lines', () => {
      const blocks = parseMarkdownToNotionBlocks('Line one\n\nLine two')
      expect(blocks).toHaveLength(2)
      expect(blocks[0].type).toBe('paragraph')
      expect(blocks[1].type).toBe('paragraph')
    })
  })

  describe('bold text', () => {
    it('should parse bold text with annotations', () => {
      const blocks = parseMarkdownToNotionBlocks('Hello **world** today')
      expect(blocks).toHaveLength(1)
      const richText = blocks[0].paragraph!.rich_text
      expect(richText).toHaveLength(3)
      expect(richText[0].text.content).toBe('Hello ')
      expect(richText[0].annotations).toBeUndefined()
      expect(richText[1].text.content).toBe('world')
      expect(richText[1].annotations).toEqual({ bold: true })
      expect(richText[2].text.content).toBe(' today')
    })

    it('should handle text that is entirely bold', () => {
      const blocks = parseMarkdownToNotionBlocks('**all bold**')
      const richText = blocks[0].paragraph!.rich_text
      expect(richText).toHaveLength(1)
      expect(richText[0].text.content).toBe('all bold')
      expect(richText[0].annotations).toEqual({ bold: true })
    })
  })

  describe('bullet lists', () => {
    it('should parse bullet list items with dash', () => {
      const blocks = parseMarkdownToNotionBlocks('- Item one\n- Item two')
      expect(blocks).toHaveLength(2)
      expect(blocks[0].type).toBe('bulleted_list_item')
      expect(blocks[0].bulleted_list_item!.rich_text[0].text.content).toBe('Item one')
      expect(blocks[1].type).toBe('bulleted_list_item')
    })

    it('should parse bullet list items with asterisk', () => {
      const blocks = parseMarkdownToNotionBlocks('* Item one\n* Item two')
      expect(blocks).toHaveLength(2)
      expect(blocks[0].type).toBe('bulleted_list_item')
      expect(blocks[1].type).toBe('bulleted_list_item')
    })

    it('should nest indented bullet items as children', () => {
      const blocks = parseMarkdownToNotionBlocks('- Parent\n  - Child')
      expect(blocks).toHaveLength(1)
      expect(blocks[0].type).toBe('bulleted_list_item')
      expect(blocks[0].bulleted_list_item!.children).toHaveLength(1)
      expect(blocks[0].bulleted_list_item!.children![0].type).toBe('bulleted_list_item')
      expect(
        blocks[0].bulleted_list_item!.children![0].bulleted_list_item!.rich_text[0].text.content
      ).toBe('Child')
    })
  })

  describe('numbered lists', () => {
    it('should parse numbered list items', () => {
      const blocks = parseMarkdownToNotionBlocks('1. First\n2. Second\n3. Third')
      expect(blocks).toHaveLength(3)
      expect(blocks[0].type).toBe('numbered_list_item')
      expect(blocks[0].numbered_list_item!.rich_text[0].text.content).toBe('First')
      expect(blocks[2].numbered_list_item!.rich_text[0].text.content).toBe('Third')
    })

    it('should nest indented numbered items as children', () => {
      const blocks = parseMarkdownToNotionBlocks('1. Parent\n   1. Child')
      expect(blocks).toHaveLength(1)
      expect(blocks[0].numbered_list_item!.children).toHaveLength(1)
      expect(blocks[0].numbered_list_item!.children![0].type).toBe('numbered_list_item')
    })
  })

  describe('quotes', () => {
    it('should parse blockquotes', () => {
      const blocks = parseMarkdownToNotionBlocks('> This is a quote')
      expect(blocks).toHaveLength(1)
      expect(blocks[0].type).toBe('quote')
      expect(blocks[0].quote!.rich_text[0].text.content).toBe('This is a quote')
    })

    it('should handle quote without space after >', () => {
      const blocks = parseMarkdownToNotionBlocks('>No space')
      expect(blocks).toHaveLength(1)
      expect(blocks[0].type).toBe('quote')
      expect(blocks[0].quote!.rich_text[0].text.content).toBe('No space')
    })
  })

  describe('mixed content', () => {
    it('should parse a document with multiple block types', () => {
      const md = `# Title

Some paragraph text.

- Bullet one
- Bullet two

> A quote

1. Numbered item`

      const blocks = parseMarkdownToNotionBlocks(md)
      expect(blocks[0].type).toBe('heading_1')
      expect(blocks[1].type).toBe('paragraph')
      expect(blocks[2].type).toBe('bulleted_list_item')
      expect(blocks[3].type).toBe('bulleted_list_item')
      expect(blocks[4].type).toBe('quote')
      expect(blocks[5].type).toBe('numbered_list_item')
    })

    it('should return empty array for empty input', () => {
      const blocks = parseMarkdownToNotionBlocks('')
      expect(blocks).toHaveLength(0)
    })

    it('should set object to block on all items', () => {
      const blocks = parseMarkdownToNotionBlocks('# Heading\nParagraph\n- List')
      blocks.forEach((block) => {
        expect(block.object).toBe('block')
      })
    })
  })
})

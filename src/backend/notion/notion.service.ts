import { Client } from '@notionhq/client'
import {
  BlockObjectRequest,
  CreatePageParameters,
  CreatePageResponse,
  QueryDataSourceResponse,
  UpdatePageParameters,
  QueryDataSourceParameters
} from '@notionhq/client/build/src/api-endpoints'
import { Book, Bookmark, NotionBlock } from '../models'
import { fetchBookCover, parseMarkdownToNotionBlocks } from '../utils'
import { env } from '../../config/env.config'

export interface ExistingPage {
  id: string
  title: string
  lastEditedTime: string
}

export class NotionService {
  private notion: Client
  private dataSourceId: string

  constructor() {
    this.notion = new Client({ auth: env.NOTION_API_KEY })
    this.dataSourceId = env.NOTION_DATA_SOURCE_ID
  }

  async getOrCreatePage(book: Book): Promise<{ parentPageId: string; highlightPageId: string }> {
    console.log('book: ', book.imageId)
    const coverUrl = await fetchBookCover(book.imageId ?? '')

    const properties: NonNullable<CreatePageParameters['properties']> = {
      Title: { title: [{ text: { content: book.bookTitle } }] },
      Category: { select: { name: 'Books' } },
      'Read Percent': { number: book.readPercent }
    }

    if (book.author) {
      properties.Author = { rich_text: [{ text: { content: book.author } }] }
    }
    if (book.publisher) {
      properties.Publisher = {
        rich_text: [{ text: { content: book.publisher } }]
      }
    }
    if (book.isbn) {
      properties.ISBN = { rich_text: [{ text: { content: book.isbn } }] }
    }
    if (book.subtitle) {
      properties.Subtitle = {
        rich_text: [{ text: { content: book.subtitle } }]
      }
    }

    const existingPage = await this._queryExistingPage(book.bookTitle)

    if (existingPage) {
      console.info(`Updating existing page for book: ${book.bookTitle} | ID: ${existingPage.id}`)
      return this._updateExistingPage(existingPage.id, coverUrl, properties)
    } else {
      console.info(`Creating new page for book: ${book.bookTitle}`)
      return this._createNewPage(coverUrl, properties)
    }
  }

  private async _queryExistingPage(
    bookTitle: string
  ): Promise<QueryDataSourceResponse['results'][number] | undefined> {
    const queryParams: QueryDataSourceParameters = {
      data_source_id: this.dataSourceId,
      filter: {
        property: 'Title',
        title: {
          equals: bookTitle
        }
      }
    }

    const response = await this.notion.dataSources.query(queryParams)
    return response.results[0]
  }

  private async _createNewPage(
    coverUrl: string,
    properties: NonNullable<CreatePageParameters['properties']>
  ): Promise<{ parentPageId: string; highlightPageId: string }> {
    const parentPage = await this._createMainPage(coverUrl, properties)
    const highlightPage = await this._createHighlightPage(parentPage.id)
    return { parentPageId: parentPage.id, highlightPageId: highlightPage.id }
  }

  private async _createMainPage(
    coverUrl: string,
    properties: NonNullable<CreatePageParameters['properties']>
  ): Promise<CreatePageResponse> {
    const createPageParams: CreatePageParameters = {
      parent: { data_source_id: this.dataSourceId },
      cover: { type: 'external', external: { url: coverUrl } },
      icon: { type: 'external', external: { url: coverUrl } },
      properties: properties
    }

    return this.notion.pages.create(createPageParams)
  }

  private async _createHighlightPage(parentPageId: string): Promise<CreatePageResponse> {
    const createPageParams: CreatePageParameters = {
      parent: { type: 'page_id', page_id: parentPageId },
      properties: { title: { title: [{ text: { content: 'Highlights' } }] } }
    }
    return this.notion.pages.create(createPageParams)
  }

  private async _updateExistingPage(
    pageId: string,
    coverUrl: string,
    properties: NonNullable<CreatePageParameters['properties']>
  ): Promise<{ parentPageId: string; highlightPageId: string }> {
    const updatePageParams: UpdatePageParameters = {
      page_id: pageId,
      cover: { type: 'external', external: { url: coverUrl } },
      icon: { type: 'external', external: { url: coverUrl } },
      properties: properties
    }
    await this.notion.pages.update(updatePageParams)

    await this._archiveOldHighlights(pageId)
    const highlightPage = await this._createHighlightPage(pageId)
    return { parentPageId: pageId, highlightPageId: highlightPage.id }
  }

  private async _archiveOldHighlights(pageId: string): Promise<void> {
    if (!pageId) {
      return
    }

    try {
      const originalHighlights = await this.notion.blocks.children.list({
        block_id: pageId
      })

      // Check if the page has a Highlights block
      const highlightPage = originalHighlights.results.find((result) => {
        if (result.id === pageId) {
          return true
        }
        return false
      })

      if (!highlightPage?.id) {
        return
      }

      await this.notion.pages.update({
        page_id: highlightPage.id,
        archived: true
      })
    } catch (error) {
      console.error('Error archiving old highlights:', error)
    }
  }

  async syncBookmarks(highlightPageId: string, bookmarks: Bookmark[]): Promise<void> {
    const bookmarkBlocks = this._prepareBookmarkBlocks(bookmarks)
    await this.syncBlocks(highlightPageId, bookmarkBlocks)
  }

  private _prepareBookmarkBlocks(bookmarks: Bookmark[]): NotionBlock[] {
    const blocks: NotionBlock[] = []
    for (const bookmark of bookmarks) {
      let content = ''
      if (bookmark.highlight) {
        content = bookmark.highlight.trim().replace(/\n/g, ' ')
      }

      const blockType = bookmark.type === 'highlight' ? 'paragraph' : 'quote'

      if (bookmark.type !== 'highlight' && bookmark.annotation) {
        const annotation = bookmark.annotation.trim()
        content = content ? `${annotation}\n${content}` : annotation
      }

      if (content) {
        blocks.push({
          object: 'block',
          type: blockType,
          [blockType]: {
            rich_text: [{ type: 'text', text: { content } }]
          }
        } as NotionBlock)
      }
    }
    return blocks
  }

  async syncBlocks(pageId: string, blocks: NotionBlock[]): Promise<void> {
    const typedBlocks = blocks as unknown as BlockObjectRequest[]
    for (let i = 0; i < blocks.length; i += 100) {
      const batch = typedBlocks.slice(i, i + 100)
      try {
        await this.notion.blocks.children.append({
          block_id: pageId,
          children: batch
        })
      } catch (error) {
        console.error('Error syncing blocks to Notion:', error)
      }
    }
  }

  async syncSummary(pageId: string, summary: string): Promise<void> {
    if (env.SUMMARIZE_ENABLED) {
      const summaryBlocks = parseMarkdownToNotionBlocks(summary)
      await this.syncBlocks(pageId, summaryBlocks)
    }
  }

  async deletePage(pageId: string): Promise<{ success: boolean; message: string }> {
    try {
      await this.notion.pages.update({
        page_id: pageId,
        archived: true
      })
      const message = `Page ${pageId} archived successfully`
      console.info(message)
      return { success: true, message }
    } catch (error) {
      console.error('Error archiving page:', error)
      throw error
    }
  }

  async queryExistingPages(bookTitles: string[]): Promise<ExistingPage[]> {
    const existingPages: ExistingPage[] = []

    for (const title of bookTitles) {
      const page = await this._queryExistingPage(title)
      if (page && 'last_edited_time' in page) {
        existingPages.push({
          id: page.id,
          title: title,
          lastEditedTime: page.last_edited_time
        })
      }
    }

    return existingPages
  }
}

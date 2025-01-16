import { Client } from '@notionhq/client';
import {
    CreatePageParameters,
    UpdatePageParameters,
    QueryDatabaseParameters,
} from '@notionhq/client/build/src/api-endpoints';
import { Book, Bookmark } from '../models';
import { fetchBookCover, parseMarkdownToNotionBlocks } from '../utils';
import { BlockObjectRequest } from '@notionhq/client/build/src/api-endpoints';

export class NotionService {
    private notion: Client;

    constructor(private notionApiKey: string, private notionDbId: string) {
        this.notion = new Client({ auth: this.notionApiKey });
    }

    async getOrCreatePage(book: Book): Promise<{ parentPageId: string; highlightPageId: string; }> {
        const coverUrl = await fetchBookCover(book.bookTitle, book.isbn);

        const properties: any = {
            Title: { title: [{ text: { content: book.bookTitle } }] },
            Category: { select: { name: 'Books' } },
            Author: { rich_text: [{ text: { content: book.author } }] },
            Publisher: { rich_text: [{ text: { content: book.publisher } }] },
            ISBN: { rich_text: [{ text: { content: book.isbn } }] },
            'Read Percent': { number: book.readPercent },
        };

        if (book.subtitle) {
            properties.Subtitle = { rich_text: [{ text: { content: book.subtitle } }] };
        }

        const existingPage = await this._queryExistingPage(book.bookTitle);

        if (existingPage) {
            console.info(`Updating existing page for book: ${book.bookTitle}`);
            return this._updateExistingPage(existingPage.id, coverUrl, properties);
        } else {
            console.info(`Creating new page for book: ${book.bookTitle}`);
            return this._createNewPage(coverUrl, properties);
        }
    }

    private async _queryExistingPage(bookTitle: string) {
        const queryParams: QueryDatabaseParameters = {
            database_id: this.notionDbId,
            filter: {
                property: 'Title',
                title: {
                    equals: bookTitle,
                },
            },
        };

        const response = await this.notion.databases.query(queryParams);
        return response.results[0];
    }

    private async _createNewPage(coverUrl: string, properties: any): Promise<{ parentPageId: string; highlightPageId: string; }> {
        const parentPage = await this._createMainPage(coverUrl, properties);
        const highlightPage = await this._createHighlightPage(parentPage.id);
        return { parentPageId: parentPage.id, highlightPageId: highlightPage.id };
    }

    private async _createMainPage(coverUrl: string, properties: any) {
        const createPageParams: CreatePageParameters = {
            parent: { database_id: this.notionDbId },
            cover: { type: 'external', external: { url: coverUrl } },
            icon: { type: 'external', external: { url: coverUrl } },
            properties: properties,
        };

        return this.notion.pages.create(createPageParams);
    }

    private async _createHighlightPage(parentPageId: string) {
        const createPageParams: CreatePageParameters = {
            parent: { type: 'page_id', page_id: parentPageId },
            properties: { title: { title: [{ text: { content: 'Highlights' } }] } },
        };
        return this.notion.pages.create(createPageParams);
    }

    private async _updateExistingPage(pageId: string, coverUrl: string, properties: any): Promise<{ parentPageId: string; highlightPageId: string; }> {
        const updatePageParams: UpdatePageParameters = {
            page_id: pageId,
            cover: { type: 'external', external: { url: coverUrl } },
            icon: { type: 'external', external: { url: coverUrl } },
            properties: properties,
        };
        await this.notion.pages.update(updatePageParams);

        await this._archiveOldHighlights(pageId);
        const highlightPage = await this._createHighlightPage(pageId);
        return { parentPageId: pageId, highlightPageId: highlightPage.id };
    }

    private async _archiveOldHighlights(pageId: string) {
        const originalHighlights = await this.notion.blocks.children.list({ block_id: pageId });
        if (originalHighlights.results.length > 0) {
            await this.notion.pages.update({
                page_id: originalHighlights.results[0].id,
                archived: true,
            });
        }
    }

    async syncBookmarks(highlightPageId: string, bookmarks: Bookmark[]): Promise<void> {
        const bookmarkBlocks = this._prepareBookmarkBlocks(bookmarks);
        await this.syncBlocks(highlightPageId, bookmarkBlocks);
    }

    private _prepareBookmarkBlocks(bookmarks: Bookmark[]): BlockObjectRequest[] {
        const blocks: BlockObjectRequest[] = [];
        for (const bookmark of bookmarks) {
            let content = '';
            if (bookmark.highlight) {
                content = bookmark.highlight.trim().replace(/\n/g, ' ');
            }

            const blockType = bookmark.type === 'highlight' ? 'paragraph' : 'quote';

            if (bookmark.type !== 'highlight' && bookmark.annotation) {
                const annotation = bookmark.annotation.trim();
                content = content ? `${annotation}\n${content}` : annotation;
            }

            if (content) {
                blocks.push({
                    object: 'block',
                    type: blockType,
                    [blockType]: {
                        rich_text: [{ type: 'text', text: { content } }],
                    },
                });
            }
        }
        return blocks;
    }

    async syncBlocks(pageId: string, blocks: BlockObjectRequest[]): Promise<void> {
        for (let i = 0; i < blocks.length; i += 100) {
            const batch = blocks.slice(i, i + 100);
            try {
                await this.notion.blocks.children.append({
                    block_id: pageId,
                    children: batch,
                });
            } catch (error) {
                console.error('Error syncing blocks to Notion:', error);
            }
        }
    }

    async syncSummary(pageId: string, summary: string): Promise<void> {
        const summaryBlocks = parseMarkdownToNotionBlocks(summary);
        await this.syncBlocks(pageId, summaryBlocks);
    }
}
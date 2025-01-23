import { contextBridge } from 'electron';
import { electronAPI } from '@electron-toolkit/preload';
import { KoboService } from '../backend/kobo/kobo.service';
import { NotionService } from '../backend/notion/notion.service';
import { GeminiService } from '../backend/llm_integration/llm_integration.service';
import { env } from '../config/env.config';

const koboService = new KoboService();
const notionService = new NotionService();
const geminiService = new GeminiService();

// Custom APIs for renderer
const api = {
  getBooks: async () => {
    await koboService.connect();
    const books = await koboService.getBooks();
    await koboService.close();
    return books;
  },
  exportBook: async (book) => {
    await koboService.connect();
    const bookmarks = await koboService.getBookmarks(book.bookTitle);

    const { parentPageId, highlightPageId } = await notionService.getOrCreatePage(book);
    await notionService.syncBookmarks(highlightPageId, bookmarks);

    if (env.SUMMARIZE_ENABLED) {
      const summary = await geminiService.summarizeBookmarks(
        book.bookTitle,
        bookmarks,
        env.SUMMARIZE_LANGUAGE
      );
      await notionService.syncSummary(highlightPageId, summary);
    }

    await koboService.close();
  }
};

// Use `contextBridge` APIs to expose Electron APIs to
// renderer only if context isolation is enabled, otherwise
// just add to the DOM global.
if (process.contextIsolated) {
  try {
    contextBridge.exposeInMainWorld('electron', electronAPI);
    contextBridge.exposeInMainWorld('api', api);
    contextBridge.exposeInMainWorld('env', env);
  } catch (error) {
    console.error(error);
  }
} else {
  // @ts-ignore (define in dts)
  window.electron = electronAPI;
  // @ts-ignore (define in dts)
  window.api = api;
  // @ts-ignore (define in dts)
  window.env = env;
}

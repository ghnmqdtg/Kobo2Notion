import { contextBridge, ipcRenderer } from 'electron';
import { electronAPI } from '@electron-toolkit/preload';
import { KoboService } from '../backend/kobo/kobo.service';
import { NotionService } from '../backend/notion/notion.service';
import { GeminiService } from '../backend/llm_integration/llm_integration.service';
import { env } from '../config/env.config';
import { fetchBookCover } from '../backend/utils';
import { Book } from '../backend/models';

const koboService = new KoboService();
const notionService = new NotionService();
const geminiService = new GeminiService();

// Reload env values once the env file is updated
const reloadEnvValues = (entries: { key: string; value: string }[]) => {
  entries.forEach(({ key, value }) => {
    // Update the env object
    env[key] = value;
    // Update process.env
    process.env[key] = value;
  });
  // Reload the page after env values are updated
  setTimeout(() => window.location.reload(), 100);
};

// Custom APIs for renderer
const api = {
  getBooks: async () => {
    await koboService.connect();
    const books = await koboService.getBooks();
    // If we close the connection here, when the user refreshes the page, the app will not work.
    // await koboService.close();
    return books;
  },
  exportBook: async (book: Book) => {
    await koboService.connect();
    const bookmarks = await koboService.getBookmarks(book.bookTitle);

    const { parentPageId, highlightPageId } = await notionService.getOrCreatePage(book);
    await notionService.syncBookmarks(highlightPageId, bookmarks);

    return { parentPageId, highlightPageId };
  },
  summarizeBook: async (book: Book, parentPageId: string) => {
    await koboService.connect();
    const bookmarks = await koboService.getBookmarks(book.bookTitle);

    const summary = await geminiService.summarizeBookmarks(
      book.bookTitle,
      bookmarks,
      env.SUMMARIZE_LANGUAGE
    );
    await notionService.syncSummary(parentPageId, summary);
  },
  fetchBookCover: async (imageId: string) => {
    return fetchBookCover(imageId);
  },
  updateEnvValue: async (entries: { key: string; value: string }[]) => {
    try {
      ipcRenderer.send('update-env', entries);
      
      // Listen for the confirmation that env was updated
      return new Promise((resolve, reject) => {
        ipcRenderer.once('env-change', (_event, updatedEntries) => {
          reloadEnvValues(updatedEntries);
          resolve(true);
        });

        // Add timeout to prevent hanging
        setTimeout(() => reject(new Error('Timeout updating env')), 5000);
      });
    } catch (error) {
      console.error('Error updating env value:', error);
      return false;
    }
  },
  openFileDialog: async () => {
    return ipcRenderer.invoke('open-file-dialog');
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

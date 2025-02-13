import { ElectronAPI } from '@electron-toolkit/preload';
import { Book } from '../backend/models';
import { EnvironmentConfig } from '../config/env.config';

export interface IElectronAPI {
  getBooks: () => Promise<Book[]>;
  exportBook: (book: Book) => Promise<{ parentPageId: string, highlightPageId: string }>;
  summarizeBook: (book: Book, parentPageId: string) => Promise<void>;
  fetchBookCover: (imageId: string) => Promise<string>;
  updateEnvValue: (entries: { key: string, value: string }[]) => Promise<boolean>;
  openFileDialog: () => Promise<string | null>;
}

declare global {
  interface Window {
    electron: ElectronAPI;
    api: IElectronAPI;
    env: EnvironmentConfig;
  }
}

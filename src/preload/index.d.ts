import { ElectronAPI } from '@electron-toolkit/preload';
import { Book } from '../backend/models';
import { EnvironmentConfig } from '../config/env.config';

export interface IElectronAPI {
  getBooks: () => Promise<Book[]>;
  exportBook: (book: Book) => Promise<void>;
  fetchBookCover: (imageId: string) => Promise<string>;
}

declare global {
  interface Window {
    electron: ElectronAPI;
    api: IElectronAPI;
    env: EnvironmentConfig;
  }
}

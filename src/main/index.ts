import { app, shell, BrowserWindow, ipcMain, globalShortcut, dialog, Menu, MenuItemConstructorOptions } from 'electron';
import { join } from 'path';
import { electronApp, optimizer, is } from '@electron-toolkit/utils';
import icon from '../../resources/icon.png?asset';
import fs from 'fs/promises';
import path from 'path';
import dotenv from 'dotenv';

const envPath = path.resolve(__dirname, '../../.env');
const exampleEnvPath = path.resolve(__dirname, '../../.env.example');

async function updateEnvFile(entries: { key: string, value: string }[]): Promise<void> {
  const envPath = path.resolve(__dirname, '../../.env');
  let content: string;
  
  try {
    content = await fs.readFile(envPath, 'utf-8')
  } catch (error) {
    content = '';
    console.error('Failed to read .env file:', error);
  }

  let lines = content.split('\n');

  // Process all entries and update corresponding lines
  entries.forEach((entry) => {
    const matchingLineIndex = lines.findIndex((line) => 
      line.match(new RegExp(entry.key))
    );

    if (matchingLineIndex !== -1) {
      lines[matchingLineIndex] = `${entry.key}=${entry.value}`;
    }
  });

  await fs.writeFile(envPath, lines.join('\n'));

  console.log('Updated env file');
}

function cleanEnvKeys(): void {
  console.log('cleaning env keys: ', process.env.NOTION_API, process.env.NOTION_DB, process.env.GEMINI_API, process.env.GEMINI_MODEL, process.env.SUMMARIZE_ENABLED, process.env.SUMMARIZE_LANGUAGE);
  delete process.env.SQLITE_SOURCE;
  delete process.env.NOTION_API;
  delete process.env.NOTION_DB;
  delete process.env.GEMINI_API;
  delete process.env.GEMINI_MODEL;
  delete process.env.SUMMARIZE_ENABLED;
  delete process.env.SUMMARIZE_LANGUAGE;
  console.log('cleaned env keys: ', process.env.NOTION_API, process.env.NOTION_DB, process.env.GEMINI_API, process.env.GEMINI_MODEL, process.env.SUMMARIZE_ENABLED, process.env.SUMMARIZE_LANGUAGE);
}

async function ensureEnvFile(): Promise<void> {
  try {
    await fs.access(envPath);
  } catch {
    // If .env doesn't exist, copy from .env.example
    try {
      const exampleContent = await fs.readFile(exampleEnvPath, 'utf-8');
      await fs.writeFile(envPath, exampleContent);
    } catch (error) {
      console.error('Failed to create .env file:', error);
    }
  }
}

function createMenu(): void {
    const isMac = process.platform === 'darwin';

    const template = [
        // App menu (macOS only)
        ...(isMac ? [{
            label: 'Kobo2Notion',
            submenu: [
                { role: 'about' },
                { type: 'separator' },
                { role: 'services' },
                { type: 'separator' },
                { role: 'hide' },
                { role: 'hideOthers' },
                { role: 'unhide' },
                { type: 'separator' },
                { role: 'quit' }
            ]
        }] : []),
        // View menu
        {
            label: 'View',
            submenu: [
                { role: 'reload' },
                { role: 'forceReload' },
                { type: 'separator' },
                { role: 'resetZoom' },
                { role: 'zoomIn' },
                { role: 'zoomOut' },
                { type: 'separator' },
                { role: 'togglefullscreen' }
            ]
        },
        // Window menu
        {
            label: 'Window',
            submenu: [
                { role: 'minimize' },
                { role: 'zoom' }
            ]
        }
    ];

    const menu = Menu.buildFromTemplate(template as MenuItemConstructorOptions[]);
    Menu.setApplicationMenu(menu);
}

function createWindow(): void {
  // Create the browser window.
  const mainWindow = new BrowserWindow({
    title: 'Kobo2Notion',
    width: 1280,
    height: 900,
    show: false,
    fullscreen: true,
    autoHideMenuBar: true,
    ...(process.platform === 'linux' ? { icon } : {}),
    webPreferences: {
      preload: join(__dirname, '../preload/index.js'),
      sandbox: false,
    }
  });

  mainWindow.on('ready-to-show', () => {
    mainWindow.show();
  });

  // Dev tools
  // mainWindow.webContents.openDevTools();

  app.on('browser-window-focus', () => {
    globalShortcut.register('f5', function () {
      mainWindow.reload();
    });

    globalShortcut.register('CommandOrControl+R', function () {
      mainWindow.reload();
    });
  });

  app.on('browser-window-blur', () => {
    globalShortcut.unregisterAll();
  });

  mainWindow.webContents.setWindowOpenHandler((details) => {
    shell.openExternal(details.url);
    return { action: 'deny' };
  });

  // HMR for renderer base on electron-vite cli.
  // Load the remote URL for development or the local html file for production.
  if (is.dev && process.env['ELECTRON_RENDERER_URL']) {
    mainWindow.loadURL(process.env['ELECTRON_RENDERER_URL']);
  } else {
    mainWindow.loadFile(join(__dirname, '../renderer/index.html'));
  }

  // Update IPC handler to send confirmation
  ipcMain.on('update-env', async (event, entries: { key: string; value: string }[]) => {
    try {
      await updateEnvFile(entries);
      // Reload the env file
      dotenv.config({ path: envPath, override: true });
      // Send confirmation back to renderer
      event.reply('env-change', entries)
    } catch (error) {
      console.error('Error updating env:', error);
      event.reply('env-change-error', error);
    }
  });

  // Add file dialog handler
  ipcMain.handle('open-file-dialog', async () => {
    const result = await dialog.showOpenDialog({
      properties: ['openFile'],
      filters: [
        { name: 'SQLite Database', extensions: ['sqlite'] }
      ]
    });
    
    if (!result.canceled && result.filePaths.length > 0) {
      return result.filePaths[0];
    }
    return null;
  });
}

function initializeApp(): void {
  // This method will be called when Electron has finished
  // initialization and is ready to create browser windows.
  // Some APIs can only be used after this event occurs.
  app.whenReady().then(async () => {
    // Set app user model id for windows
    electronApp.setAppUserModelId('com.electron');

    // Create menu
    createMenu();

    // Default open or close DevTools by F12 in development
    // and ignore CommandOrControl + R in production.
    // see https://github.com/alex8088/electron-toolkit/tree/master/packages/utils
    app.on('browser-window-created', (_, window) => {
      optimizer.watchWindowShortcuts(window);
    });

    createWindow();

    app.on('activate', function () {
      // On macOS it's common to re-create a window in the app when the
      // dock icon is clicked and there are no other windows open.
      if (BrowserWindow.getAllWindows().length === 0) createWindow();
    });
  });

  // Quit when all windows are closed, except on macOS. There, it's common
  // for applications and their menu bar to stay active until the user quits
  // explicitly with Cmd + Q.
  app.on('window-all-closed', () => {
    if (process.platform !== 'darwin') {
      app.quit();
    }
  });
}

ensureEnvFile().then(() => {
  cleanEnvKeys();

  // Load environment variables from .env file
  dotenv.config({ path: envPath });
  console.log('cleaned env keys: ', process.env.NOTION_API, process.env.NOTION_DB, process.env.GEMINI_API, process.env.GEMINI_MODEL, process.env.SUMMARIZE_ENABLED, process.env.SUMMARIZE_LANGUAGE);

  initializeApp();
});

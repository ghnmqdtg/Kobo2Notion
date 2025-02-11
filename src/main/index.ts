import { app, shell, BrowserWindow, ipcMain, globalShortcut } from 'electron';
import { join } from 'path';
import { electronApp, optimizer, is } from '@electron-toolkit/utils';
import icon from '../../resources/icon.png?asset';
import fs from 'fs/promises';
import path from 'path';

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
  const envPath = path.resolve(__dirname, '../../.env');
  const exampleEnvPath = path.resolve(__dirname, '../../.env.example');

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

function createWindow(): void {
  // Create the browser window.
  const mainWindow = new BrowserWindow({
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

  // Add IPC handlers
  ipcMain.on('update-env', async (_, { key, value }) => {
    console.log('update-env', key, value);
    await updateEnvFile(key, value);
  });
}

// This method will be called when Electron has finished
// initialization and is ready to create browser windows.
// Some APIs can only be used after this event occurs.
app.whenReady().then(async () => {
  await ensureEnvFile();
  // Set app user model id for windows
  electronApp.setAppUserModelId('com.electron');

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

// In this file you can include the rest of your app"s specific main process
// code. You can also put them in separate files and require them here.

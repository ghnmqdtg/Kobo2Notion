import { autoUpdater } from 'electron-updater'
import { dialog } from 'electron'

export function initAutoUpdater(): void {
  autoUpdater.logger = null

  autoUpdater.on('checking-for-update', () => {
    console.log('[updater] Checking for update...')
  })

  autoUpdater.on('update-available', (info) => {
    console.log('[updater] Update available:', info.version)
  })

  autoUpdater.on('update-not-available', () => {
    console.log('[updater] Update not available.')
  })

  autoUpdater.on('download-progress', (progress) => {
    console.log(`[updater] Download progress: ${progress.percent.toFixed(1)}%`)
  })

  autoUpdater.on('update-downloaded', (info) => {
    console.log('[updater] Update downloaded:', info.version)
    dialog
      .showMessageBox({
        type: 'info',
        title: 'Update Ready',
        message: `Version ${info.version} has been downloaded. Restart now to apply the update?`,
        buttons: ['Restart', 'Later']
      })
      .then((result) => {
        if (result.response === 0) {
          autoUpdater.quitAndInstall()
        }
      })
  })

  autoUpdater.on('error', (err) => {
    console.error('[updater] Error:', err)
  })

  autoUpdater.checkForUpdatesAndNotify()
}

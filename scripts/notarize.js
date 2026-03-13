const { notarize } = require('@electron/notarize')

exports.default = async function notarizing(context) {
  const { electronPlatformName, appOutDir } = context

  if (electronPlatformName !== 'darwin' || process.env.SKIP_NOTARIZE === 'true') {
    console.log('Skipping notarization')
    return
  }

  const appName = context.packager.appInfo.productFilename
  const appPath = `${appOutDir}/${appName}.app`

  const appleId = process.env.APPLE_ID
  const appleIdPassword = process.env.APPLE_APP_SPECIFIC_PASSWORD
  const teamId = process.env.APPLE_TEAM_ID

  if (!appleId || !appleIdPassword || !teamId) {
    throw new Error(
      'Missing required env vars: APPLE_ID, APPLE_APP_SPECIFIC_PASSWORD, APPLE_TEAM_ID'
    )
  }

  console.log(`Notarizing ${appPath}...`)

  await notarize({
    appPath,
    appleId,
    appleIdPassword,
    teamId
  })

  console.log('Notarization complete')
}

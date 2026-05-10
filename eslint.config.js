const { configs } = require('@electron-toolkit/eslint-config-ts')
const prettierConfig = require('@electron-toolkit/eslint-config-prettier')

module.exports = [
  {
    ignores: ['node_modules/**', 'dist/**', 'out/**', 'config/.eslintrc.cjs']
  },
  ...configs.recommended,
  prettierConfig,
  {
    // CJS config files use require() — allow it in plain JS files
    files: ['**/*.js', '**/*.cjs', '**/*.mjs'],
    rules: {
      '@typescript-eslint/no-require-imports': 'off'
    }
  },
  {
    // react/prop-types is not applicable (TypeScript project, plugin not loaded)
    rules: {
      'react/prop-types': 'off'
    }
  }
]

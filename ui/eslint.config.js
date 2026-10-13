import js from '@eslint/js';
import globals from 'globals';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import tseslint from 'typescript-eslint';
import i18n from './eslint-rules/no-hardcoded-ui-text.js';

export default tseslint.config(
  { ignores: ['dist', 'node_modules', 'screenshots'] },
  {
    extends: [js.configs.recommended, ...tseslint.configs.recommended],
    files: ['**/*.{ts,tsx}'],
    languageOptions: { ecmaVersion: 2022, globals: { ...globals.browser, ...globals.node } },
    plugins: { 'react-hooks': reactHooks, 'react-refresh': reactRefresh },
    rules: {
      ...reactHooks.configs.recommended.rules,
      'react-refresh/only-export-components': ['warn', { allowConstantExport: true }],
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],
    },
  },
  {
    // Bilingual console: no hard-coded user-visible strings in screens and components.
    files: ['src/pages/**/*.tsx', 'src/components/**/*.tsx', 'src/shell/**/*.tsx', 'src/App.tsx'],
    ignores: ['src/pages/docs/**'],
    plugins: { i18n },
    rules: { 'i18n/no-hardcoded-ui-text': 'error' },
  },
);

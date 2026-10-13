/**
 * Flags obvious hard-coded user-visible text in JSX (the console is bilingual: every visible
 * string goes through `t()` / `useT()` from src/i18n). Heuristic on purpose — it catches the
 * regressions that matter (JSX text, ternaries of literals inside JSX, labelled attributes,
 * toast text) without flagging identifiers, class names or code snippets.
 *
 * Escape hatch for a genuine technical literal: `// eslint-disable-next-line i18n/no-hardcoded-ui-text`.
 */

/** Exact trimmed strings that are fine untranslated (brand, protocol names, keys, symbols). */
const ALLOW = new Set([
  'FluxMQ', 'JSON', 'DLQ', 'TLS', 'mTLS', 'HTTP', 'HTTPS', 'URL', 'ID', 'CLI', 'API', 'REST', 'WebSocket', 'Prometheus', 'Kafka', 'RabbitMQ',
  'Ctrl', 'Esc', 'Enter', 'Tab', 'Shift', 'Alt', 'K', 'S3', 'GCS', 'Slack', 'PagerDuty', 'Telegram', 'Webhook', 'SMTP', 'CA', 'PEM', 'SAN', 'CN',
  'esc', 'Authorization', 'Bearer …', 'UTF-8', 'utf8', 'base64', 'hex', 'p50', 'p95', 'p99', 'p999', 'RTT', 'ms', 'QoS', 'ok', 'OK', 'v', 'B', 'KB', 'MB', 'GB', 'TB', 'AND', 'OR', 'NOT',
]);

/** Attributes whose string value is shown to the user. */
const ATTRS = new Set(['title', 'placeholder', 'aria-label', 'alt', 'label', 'hint', 'description', 'emptyText', 'confirmLabel', 'subtitle', 'heading', 'tooltip', 'aria-description', 'aria-roledescription', 'aria-valuetext']);

const LETTERS = /[A-Za-zА-Яа-яЁё]{2,}/;

function prose(raw, strict) {
  const s = raw.replace(/\s+/g, ' ').trim();
  if (!s || !LETTERS.test(s) || ALLOW.has(s)) return false;
  if (/[А-Яа-яЁё]/.test(s)) return true;
  // Paths, identifiers, file names: no spaces, no capitals, some punctuation (/metrics, orders.*, fmq_).
  if (!/\s/.test(s) && !/[A-Z]/.test(s) && /[./_:\-*{}]/.test(s)) return false;
  if (strict) return true;
  // A sentence-ish literal: words separated by spaces, or a Capitalised word.
  return /[A-Za-z]{2,}\s+[A-Za-z]{2,}/.test(s) || /^[A-Z][a-z]{2,}/.test(s);
}

function* literalsIn(node) {
  if (!node) return;
  switch (node.type) {
    case 'Literal':
      if (typeof node.value === 'string') yield { node, text: node.value };
      break;
    case 'TemplateLiteral':
      for (const q of node.quasis) yield { node: q, text: q.value.cooked ?? '' };
      break;
    case 'ConditionalExpression':
      yield* literalsIn(node.consequent);
      yield* literalsIn(node.alternate);
      break;
    case 'LogicalExpression':
      yield* literalsIn(node.right);
      break;
    default:
  }
}

export default {
  rules: {
    'no-hardcoded-ui-text': {
      meta: { type: 'problem', docs: { description: 'User-visible text must go through t() (src/i18n)' }, schema: [] },
      create(context) {
        const report = (node, text) => context.report({ node, message: `Hard-coded UI text "${text.trim().slice(0, 40)}" — use t() from src/i18n` });
        return {
          JSXText(node) {
            if (prose(node.value, true)) report(node, node.value);
          },
          JSXExpressionContainer(node) {
            if (node.parent?.type === 'JSXAttribute') {
              const name = typeof node.parent.name.name === 'string' ? node.parent.name.name : '';
              if (!ATTRS.has(name)) return;
            }
            for (const l of literalsIn(node.expression)) if (prose(l.text, false)) report(l.node, l.text);
          },
          JSXAttribute(node) {
            const name = typeof node.name.name === 'string' ? node.name.name : '';
            if (ATTRS.has(name) && node.value?.type === 'Literal' && typeof node.value.value === 'string' && prose(node.value.value, false)) report(node.value, node.value.value);
          },
          CallExpression(node) {
            const c = node.callee;
            const isToast = (c.type === 'Identifier' && c.name === 'toast') || (c.type === 'MemberExpression' && c.property.type === 'Identifier' && c.property.name === 'toast');
            if (!isToast) return;
            for (const a of node.arguments.slice(1)) for (const l of literalsIn(a)) if (prose(l.text, false)) report(l.node, l.text);
          },
        };
      },
    },
  },
};

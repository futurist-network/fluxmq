/**
 * Filter DSL syntax errors as the server reports them: a 400 `ApiError` whose `position` is the
 * **byte** offset (UTF-8) of the error in the submitted text. The console never parses the DSL
 * itself; it only maps that offset onto the text the user typed to point at the problem.
 */
import { ApiError, errorMessage } from '../api/client';

function utf8Len(codePoint: number): number {
  return codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4;
}

/**
 * UTF-8 byte offset → JS string index (UTF-16 code units). An offset inside a multi-byte
 * character maps to the start of that character; past the end maps to `s.length`.
 */
export function byteToIndex(s: string, byte: number): number {
  let b = 0;
  let i = 0;
  for (const c of s) {
    const len = utf8Len(c.codePointAt(0)!);
    if (byte < b + len) return i;
    b += len;
    i += c.length;
  }
  return s.length;
}

export interface FilterErrorInfo {
  /** Full server message. */
  message: string;
  /** The message without the `filter syntax error at <n>: ` prefix (the UI shows the column). */
  detail: string;
  /** JS string index into the submitted text, or null when the error has no position. */
  index: number | null;
}

/** Describe an error from a request that submitted `text` as a filter. */
export function filterErrorInfo(text: string, e: unknown): FilterErrorInfo {
  const message = errorMessage(e);
  const position = e instanceof ApiError ? e.position : null;
  if (position == null) return { message, detail: message, index: null };
  const prefix = `filter syntax error at ${position}: `;
  return { message, detail: message.startsWith(prefix) ? message.slice(prefix.length) : message, index: byteToIndex(text, position) };
}

/** The error token to highlight: from `index` to the next whitespace (at least one char). */
export function errorSpan(text: string, index: number): [number, number] {
  const i = Math.min(index, text.length);
  let j = i;
  while (j < text.length && !/\s/.test(text[j]!)) j++;
  if (j === i) j = Math.min(text.length, i + 1);
  return [i, j];
}

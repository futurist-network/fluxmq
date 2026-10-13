/** Read a design token (CSS custom property) for canvas drawing. */
export function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || '#5b7cfa';
}

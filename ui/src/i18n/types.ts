/** Dictionary value types shared by the English source and the Russian translation. */

export type Lang = 'en' | 'ru';

export const LANGS: readonly Lang[] = ['en', 'ru'];

/** English plural entry: CLDR `one` / `other`. */
export interface EnPlural {
  one: string;
  other: string;
}

/** Russian plural entry: CLDR `one` (1, 21…), `few` (2–4, 22…), `many` (5–20, 25…), `other` (fractions). */
export interface RuPlural {
  one: string;
  few: string;
  many: string;
  other: string;
}

export type Entry = string | EnPlural;

/**
 * The Russian counterpart of an English namespace: exactly the same keys (a missing or an extra
 * key is a compile error), strings stay strings, plural entries need all four Russian forms.
 */
export type RuOf<T> = { [K in keyof T]: T[K] extends string ? string : RuPlural };

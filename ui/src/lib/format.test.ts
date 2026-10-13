import { afterEach, describe, expect, it } from 'vitest';
import { fmtBackoff, fmtBytes, fmtBytesRate, fmtCount, fmtDateTime, fmtDuration, fmtInt, fmtMs, fmtNumber, fmtPercent, fmtRate, fmtRelative, fmtRetention, fmtTimeMs, splitUnit, fmtDate } from './format';
import { setLang } from '../i18n';
import { formatBackoffList, parseBackoffList } from './presets';
import { decodePayload, hexDump, tokenizeJson } from './payload';
import { validateChannelName } from '../api/types';

describe('formatters', () => {
  it('fmtCount', () => {
    expect(fmtCount(0)).toBe('0');
    expect(fmtCount(999)).toBe('999');
    expect(fmtCount(1200)).toBe('1.2K');
    expect(fmtCount(18_000)).toBe('18K');
    expect(fmtCount(184_000)).toBe('184K');
    expect(fmtCount(1_400_000)).toBe('1.4M');
    expect(fmtCount(2_100_000_000)).toBe('2.1B');
    expect(fmtCount(999_960)).toBe('1M');
    expect(fmtCount(null)).toBe('—');
  });
  it('fmtRate', () => {
    expect(fmtRate(1_400_000)).toBe('1.4M msg/s');
    expect(fmtRate(42_400, false)).toBe('42.4K');
    expect(fmtRate(0.25)).toBe('0.25 msg/s');
  });
  it('fmtMs', () => {
    expect(fmtMs(0.42)).toBe('0.42 ms');
    expect(fmtMs(2.84)).toBe('2.8 ms');
    expect(fmtMs(38.2)).toBe('38 ms');
    expect(fmtMs(1200)).toBe('1.2 s');
    expect(fmtMs(204_000)).toBe('3.4 min');
  });
  it('fmtBytes', () => {
    expect(fmtBytes(512)).toBe('512 B');
    expect(fmtBytes(4300)).toBe('4.2 KB');
    expect(fmtBytes(4.8 * 1024 ** 4)).toBe('4.8 TB');
  });
  it('fmtRelative', () => {
    const now = 1_000_000_000;
    expect(fmtRelative(now - 2000, now)).toBe('just now');
    expect(fmtRelative(now - 12_000, now)).toBe('12s ago');
    expect(fmtRelative(now - 4 * 60_000, now)).toBe('4m ago');
    expect(fmtRelative(now - 3 * 3_600_000, now)).toBe('3h ago');
    expect(fmtRelative(now - 2 * 86_400_000, now)).toBe('2d ago');
    expect(fmtRelative(now + 6000, now)).toBe('in 6s');
  });
  it('fmtDuration / retention / backoff', () => {
    expect(fmtDuration(45)).toBe('45s');
    expect(fmtDuration(3 * 3600 + 12 * 60)).toBe('3h 12m');
    expect(fmtDuration(14 * 86400 + 6 * 3600)).toBe('14d 6h');
    expect(fmtRetention(7 * 86400)).toBe('7 days');
    expect(fmtRetention(null)).toBe('Forever');
    expect(fmtBackoff(10_000)).toBe('10 s');
    expect(fmtBackoff(300_000)).toBe('5 min');
  });
  it('fmtTimeMs pads milliseconds', () => {
    const d = new Date(2026, 0, 1, 9, 5, 7, 4).getTime();
    expect(fmtTimeMs(d)).toBe('09:05:07.004');
  });
  it('splitUnit', () => {
    expect(splitUnit('42.4K msg/s')).toEqual(['42.4K', 'msg/s']);
    expect(splitUnit('7')).toEqual(['7', '']);
  });
});

describe('backoff list parsing', () => {
  it('parses units and round-trips', () => {
    expect(parseBackoffList('1s, 10s, 1m, 5m')).toEqual([1000, 10_000, 60_000, 300_000]);
    expect(parseBackoffList('500ms 2.5s')).toEqual([500, 2500]);
    expect(parseBackoffList('soon')).toBeNull();
    expect(formatBackoffList([1000, 10_000, 60_000, 300_000])).toBe('1s, 10s, 1m, 5m');
  });
});

describe('channel name validation (mirrors validate_channel_name)', () => {
  it.each([
    ['orders.events', null],
    ['', 'channel name must be 1-128 characters'],
    ['a'.repeat(129), 'channel name must be 1-128 characters'],
    ['bad name', 'channel name may only contain [a-zA-Z0-9._-]'],
    ['.hidden', "channel name may not start with '.' or end with '.consumers'/'.tail'"],
    ['x.consumers', "channel name may not start with '.' or end with '.consumers'/'.tail'"],
    ['x.tail', "channel name may not start with '.' or end with '.consumers'/'.tail'"],
  ])('%s', (name, err) => {
    expect(validateChannelName(name)).toBe(err);
  });
});

describe('payload decoding', () => {
  it('detects JSON in utf8 payloads', () => {
    const d = decodePayload({ encoding: 'utf8', data: '{"order_id":8842}' });
    expect(d.isJson).toBe(true);
    expect(d.json).toEqual({ order_id: 8842 });
  });
  it('treats binary base64 as non-text and hex-dumps it', () => {
    const d = decodePayload({ encoding: 'base64', data: btoa(String.fromCharCode(0, 1, 2, 255, 254, 0x41)) });
    expect(d.text).toBeNull();
    expect(hexDump(d.bytes)).toBe('00000000  00 01 02 ff fe 41                                 |.....A|');
  });
  it('decodes base64 that is really UTF-8 JSON', () => {
    const d = decodePayload({ encoding: 'base64', data: btoa('{"a":true}') });
    expect(d.isJson).toBe(true);
  });
  it('tokenizes JSON for highlighting without losing text', () => {
    const text = JSON.stringify({ a: 'x', b: 1.5, c: null, d: [true] }, null, 2);
    const toks = tokenizeJson(text);
    expect(toks.map((t) => t.text).join('')).toBe(text);
    expect(toks.find((t) => t.kind === 'key')?.text).toBe('"a"');
    expect(toks.some((t) => t.kind === 'number' && t.text === '1.5')).toBe(true);
    expect(toks.some((t) => t.kind === 'null')).toBe(true);
    expect(toks.some((t) => t.kind === 'bool')).toBe(true);
  });
});

describe('fmtDate', () => {
  it('includes the year', () => {
    expect(fmtDate(new Date(2027, 6, 26, 18, 0).getTime())).toBe('Jul 26, 2027');
    expect(fmtDate(null)).toBe('—');
  });
});

describe('formatters in Russian', () => {
  afterEach(() => setLang('en'));
  const NB = '\u00a0';

  it('numbers: decimal comma, space grouping, Russian compact suffixes', () => {
    setLang('ru');
    expect(fmtCount(999)).toBe('999');
    expect(fmtCount(1200)).toBe(`1,2${NB}тыс.`);
    expect(fmtCount(1_400_000)).toBe(`1,4${NB}млн`);
    expect(fmtCount(2_100_000_000)).toBe(`2,1${NB}млрд`);
    expect(fmtInt(1_284_402)).toBe(`1${NB}284${NB}402`);
    expect(fmtNumber(3.14159)).toBe('3,14');
    expect(fmtPercent(42)).toBe(`42${NB}%`);
  });

  it('rates, latency, bytes', () => {
    setLang('ru');
    expect(fmtRate(42_400)).toBe(`42,4${NB}тыс. сообщ./с`);
    expect(splitUnit(fmtRate(42_400))).toEqual([`42,4${NB}тыс.`, 'сообщ./с']);
    expect(fmtMs(2.84)).toBe('2,8 мс');
    expect(fmtMs(1200)).toBe('1,2 с');
    expect(fmtMs(204_000)).toBe('3,4 мин');
    expect(fmtBytes(4300)).toBe('4,2 КБ');
    expect(fmtBytes(4.8 * 1024 ** 4)).toBe('4,8 ТБ');
    expect(fmtBytesRate(512)).toBe('512 Б/с');
  });

  it('durations, retention and backoff', () => {
    setLang('ru');
    expect(fmtDuration(45)).toBe(`45${NB}с`);
    expect(fmtDuration(12 * 60)).toBe(`12${NB}мин`);
    expect(fmtDuration(3 * 3600 + 12 * 60)).toBe(`3${NB}ч 12${NB}мин`);
    expect(fmtDuration(14 * 86400 + 6 * 3600)).toBe(`14${NB}д 6${NB}ч`);
    expect(fmtRetention(86400)).toBe('1 день');
    expect(fmtRetention(3 * 86400)).toBe('3 дня');
    expect(fmtRetention(7 * 86400)).toBe('7 дней');
    expect(fmtRetention(21 * 86400)).toBe('21 день');
    expect(fmtRetention(24 * 3600 + 3600)).toBe('25 часов');
    expect(fmtRetention(2 * 3600)).toBe('2 часа');
    expect(fmtRetention(null)).toBe('Бессрочно');
    expect(fmtBackoff(10_000)).toBe('10 с');
    expect(fmtBackoff(300_000)).toBe('5 мин');
  });

  it('relative time reads naturally', () => {
    setLang('ru');
    const now = 1_000_000_000;
    expect(fmtRelative(now - 2000, now)).toBe('только что');
    expect(fmtRelative(now - 12_000, now)).toBe('12 секунд назад');
    expect(fmtRelative(now - 5 * 60_000, now)).toBe('5 минут назад');
    expect(fmtRelative(now - 2 * 60_000, now)).toBe('2 минуты назад');
    expect(fmtRelative(now - 21 * 60_000, now)).toBe('21 минуту назад');
    expect(fmtRelative(now - 3 * 3_600_000, now)).toBe('3 часа назад');
    expect(fmtRelative(now - 2 * 86_400_000, now)).toBe('2 дня назад');
    expect(fmtRelative(now + 6000, now)).toBe('через 6 секунд');
  });

  it('dates use Russian month names and day-first order', () => {
    const ts = new Date(2027, 6, 26, 18, 5, 9).getTime();
    expect(fmtDateTime(ts)).toBe('Jul 26, 18:05:09');
    expect(fmtDate(ts)).toBe('Jul 26, 2027');
    setLang('ru');
    expect(fmtDateTime(ts)).toBe('26 июл., 18:05:09');
    expect(fmtDate(ts)).toBe('26 июл. 2027 г.');
    // Locale-neutral DevTools-style clock stays the same.
    expect(fmtTimeMs(new Date(2026, 0, 1, 9, 5, 7, 4).getTime())).toBe('09:05:07.004');
  });

  it('switching back restores English', () => {
    setLang('ru');
    setLang('en');
    expect(fmtCount(1200)).toBe('1.2K');
    expect(fmtRelative(1_000_000_000 - 4 * 60_000, 1_000_000_000)).toBe('4m ago');
    expect(fmtInt(1_284_402)).toBe('1,284,402');
  });
});

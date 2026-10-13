import { validateChannelName } from '../api/types';
import { t, type TKey } from '../i18n';

/**
 * `validateChannelName` (api/types.ts) mirrors the broker's English messages and is shared with
 * the mock server, so it stays i18n-free; this maps its result to the console language.
 */
const MESSAGES: Record<string, TKey> = {
  'channel name must be 1-128 characters': 'channels.nameError.length',
  'channel name may only contain [a-zA-Z0-9._-]': 'channels.nameError.chars',
  "channel name may not start with '.' or end with '.consumers'/'.tail'": 'channels.nameError.reserved',
};

export function channelNameError(name: string): string | null {
  const e = validateChannelName(name);
  if (e == null) return null;
  const key = MESSAGES[e];
  return key ? t(key) : e;
}

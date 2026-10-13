import type { Entry } from '../types';

/**
 * Messages for known API error codes (`ApiError.code`). The server's own `message` is shown
 * verbatim only for codes without an entry here (it is English and may carry details).
 */
export const errors = {
  'errors.network': 'Cannot reach the FluxMQ server',
  'errors.unauthorized': 'Authentication required — the access token is missing, revoked or expired.',
  'errors.forbidden': 'Your role doesn’t allow this action.',
  'errors.not_found': 'Not found.',
  'errors.unavailable': 'The broker is temporarily unavailable. Try again in a moment.',
  'errors.internal': 'Internal server error. Check the broker logs.',
  'errors.unsupported_in_cluster': 'Not available on a clustered node.',
  'errors.unsupported_single_node': 'Only available in a cluster — this broker runs as a single node.',
  'errors.last_admin': 'This is the last active admin token. Create another admin token (or start the broker with --admin-token) before revoking it.',
  'errors.withDetail': '{message} ({detail})',
  'errors.http': 'HTTP {status}',
} satisfies Record<string, Entry>;

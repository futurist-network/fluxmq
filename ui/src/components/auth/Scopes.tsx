import type { ClientScopes, Role } from '../../api/types';
import { globMatch, validateGlob } from '../../lib/rbac';
import { scopeMatches } from '../../lib/tokenFlow';
import { ChipsInput } from '../ChipsInput';
import { useT } from '../../i18n';

/** Scopes of a token or certificate binding (same semantics). */
export function Scopes({ t: subject }: { t: { role: Role; scopes: ClientScopes } }) {
  const t = useT();
  const { publish, consume } = subject.scopes;
  const subscribe = subject.scopes.subscribe ?? [];
  if (!publish.length && !consume.length && !subscribe.length) {
    return <span className="faint">{subject.role === 'viewer' ? t('tokens.scopes.noneReadOnly') : t('tokens.scopes.allByRole')}</span>;
  }
  return (
    <div className="scope-cell">
      {publish.length > 0 && (
        <div>
          <span className="scope-k" title={t('tokens.scopes.pubTitle')}>
            {t('tokens.scopes.pub')}
          </span>
          {publish.map((g) => (
            <span key={g} className="chip static">
              {g}
            </span>
          ))}
        </div>
      )}
      {subscribe.length > 0 && (
        <div>
          <span className="scope-k" title={t('tokens.scopes.liveTitle')}>
            {t('tokens.scopes.live')}
          </span>
          {subscribe.map((g) => (
            <span key={g} className="chip static">
              {g}
            </span>
          ))}
        </div>
      )}
      {consume.length > 0 && (
        <div>
          <span className="scope-k" title={t('tokens.scopes.subTitle')}>
            {t('tokens.scopes.sub')}
          </span>
          {consume.map((g) => (
            <span key={g} className="chip static">
              {g}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

export function ScopeField({ label, hint, value, onChange, error, channels }: { label: string; hint: string; value: string[]; onChange: (v: string[]) => void; error?: string; channels: string[] }) {
  const matched = scopeMatches(value, channels, globMatch);
  const t = useT();
  return (
    <div className="field">
      <span className="field-label">{label}</span>
      <ChipsInput label={label} value={value} onChange={onChange} validate={validateGlob} placeholder={t('tokens.scopes.placeholder')} suggestions={['*', ...channels]} invalid={!!error} />
      {error ? (
        <span className="field-error">{error}</span>
      ) : (
        <span className="field-hint">
          {value.length ? (
            matched.length ? (
              <>
                {t('tokens.scopes.matches', { count: matched.length })} <span className="mono">{matched.slice(0, 4).join(', ')}</span>
                {matched.length > 4 ? ` +${matched.length - 4}` : ''}
              </>
            ) : (
              t('tokens.scopes.matchesNone')
            )
          ) : (
            hint
          )}
        </span>
      )}
    </div>
  );
}

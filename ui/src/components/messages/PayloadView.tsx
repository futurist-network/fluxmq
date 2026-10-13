import { useMemo, useState } from 'react';
import type { JsonPayload } from '../../api/types';
import { decodePayload, hexDump, tokenizeJson } from '../../lib/payload';
import { detectFormat, FORMAT_LABEL, parseCsv, parseForm, prettyXml, tokenizeXml, type PayloadFormat } from '../../lib/payloadFormat';
import { Segmented } from '../ui';
import { useT } from '../../i18n';

type View = 'pretty' | 'raw' | 'hex';
const HIGHLIGHT_LIMIT = 200_000;

function JsonHighlighted({ text }: { text: string }) {
  const tokens = useMemo(() => (text.length > HIGHLIGHT_LIMIT ? null : tokenizeJson(text)), [text]);
  if (!tokens) return <>{text}</>;
  return (
    <>
      {tokens.map((t, i) =>
        t.kind === 'ws' ? (
          t.text
        ) : (
          <span key={i} className={`syn-${t.kind}`}>
            {t.text}
          </span>
        ),
      )}
    </>
  );
}

/** XML / HTML with tags, attributes, values and comments coloured. */
function MarkupHighlighted({ text }: { text: string }) {
  const tokens = useMemo(() => (text.length > HIGHLIGHT_LIMIT ? null : tokenizeXml(text)), [text]);
  if (!tokens) return <>{text}</>;
  return (
    <>
      {tokens.map((t, i) =>
        t.kind === 'ws' || t.kind === 'text' ? (
          t.text
        ) : (
          <span key={i} className={`syn-x-${t.kind}`}>
            {t.text}
          </span>
        ),
      )}
    </>
  );
}

function FormTable({ text }: { text: string }) {
  const rows = useMemo(() => parseForm(text), [text]);
  return (
    <table className="payload-table">
      <tbody>
        {rows.map(([k, v], i) => (
          <tr key={i}>
            <th className="mono syn-key">{k}</th>
            <td className="mono">{v === '' ? <span className="faint">—</span> : v}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function CsvTable({ text }: { text: string }) {
  const rows = useMemo(() => parseCsv(text), [text]);
  const [head, ...body] = rows;
  return (
    <table className="payload-table csv">
      <thead>
        <tr>
          {head?.map((h, i) => (
            <th key={i} className="mono">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {body.map((r, i) => (
          <tr key={i}>
            {r.map((c, j) => (
              <td key={j} className="mono">
                {c}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** Format-aware: pretty JSON / XML / HTML (highlighted), form fields, CSV table → text → hex. */
export function PayloadView({ payload, truncated, contentType }: { payload: JsonPayload | string; truncated?: boolean; contentType?: string }) {
  const t = useT();
  const d = useMemo(() => decodePayload(payload), [payload]);
  const format: PayloadFormat = useMemo(() => detectFormat(d, contentType), [d, contentType]);
  const pretty = useMemo(() => {
    if (format === 'json' && d.isJson) return JSON.stringify(d.json, null, 2);
    if ((format === 'xml' || format === 'html') && d.text != null && d.text.length <= HIGHLIGHT_LIMIT) return prettyXml(d.text, format === 'html');
    return d.text;
  }, [d, format]);
  const hasPretty = format !== 'text' && format !== 'binary';
  const defaultView: View = hasPretty ? 'pretty' : d.text != null ? 'raw' : 'hex';
  const [view, setView] = useState<View>(defaultView);
  const options: { value: View; label: string }[] = [
    ...(hasPretty ? [{ value: 'pretty' as const, label: FORMAT_LABEL[format as keyof typeof FORMAT_LABEL] }] : []),
    ...(d.text != null ? [{ value: 'raw' as const, label: t('messages.payload.text') }] : []),
    { value: 'hex', label: 'Hex' },
  ];
  const active = options.some((o) => o.value === view) ? view : defaultView;
  const kind = contentType ?? (format === 'binary' ? t('messages.payload.kindBinary') : format === 'text' ? t('messages.payload.kindText') : t(`messages.payload.kind.${format}`));
  const table = active === 'pretty' && (format === 'form' || format === 'csv') && d.text != null;
  return (
    <div className="payload">
      <div className="payload-bar">
        <Segmented value={active} options={options} onChange={setView} label={t('messages.payload.viewAria')} />
        <span className="faint payload-kind">
          {kind}
          {typeof payload !== 'string' && payload.encoding === 'base64' ? ' · base64' : ''}
          {truncated ? ` · ${t('messages.payload.truncated')}` : ''}
        </span>
      </div>
      {table ? (
        <div className="payload-pre payload-table-wrap" tabIndex={0} aria-label={t('messages.payload.aria')}>
          {format === 'form' ? <FormTable text={d.text!} /> : <CsvTable text={d.text!} />}
        </div>
      ) : (
        <pre className="payload-pre" tabIndex={0} aria-label={t('messages.payload.aria')}>
          {active === 'pretty' && format === 'json' && pretty != null ? (
            <JsonHighlighted text={pretty} />
          ) : active === 'pretty' && (format === 'xml' || format === 'html') && pretty != null ? (
            <MarkupHighlighted text={pretty} />
          ) : active === 'raw' || (active === 'pretty' && d.text != null) ? (
            d.text
          ) : (
            hexDump(d.bytes)
          )}
        </pre>
      )}
    </div>
  );
}

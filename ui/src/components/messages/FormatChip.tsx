import { FORMAT_LABEL, type PayloadFormat } from '../../lib/payloadFormat';
import { useT } from '../../i18n';

/** A small coloured tag: JSON / XML / HTML / Form / CSV / TXT / BIN. */
export function FormatChip({ format }: { format: PayloadFormat }) {
  const t = useT();
  const label = format === 'text' ? 'TXT' : format === 'binary' ? 'BIN' : FORMAT_LABEL[format];
  const title = format === 'text' ? t('messages.payload.kindText') : format === 'binary' ? t('messages.payload.kindBinary') : t(`messages.payload.kind.${format}`);
  return (
    <span className={`fmt-chip fmt-${format}`} title={title}>
      {label}
    </span>
  );
}

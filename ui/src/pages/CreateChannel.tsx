import { useEffect, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router';
import { create } from 'zustand';
import {
  Activity,
  ArrowUpWideNarrow,
  ArrowLeft,
  ArrowRight,
  ArrowUpRight,
  ChevronDown,
  ChevronRight,
  KeyRound,
  LayoutTemplate,
  ListChecks,
  ListOrdered,
  Repeat,
  RotateCcw,
  Route,
  ScrollText,
  Share2,
  Webhook,
  X,
} from 'lucide-react';
import { ApiError, api, errorMessage } from '../api/client';
import { PRIORITY_LEVELS, type ChannelMode } from '../api/types';
import { useT } from '../i18n';
import { channelPath, MODE_LABEL } from '../lib/labels';
import { DEFAULT_RETRY, formatBackoffList, MAX_BYTES_PRESETS, RETENTION_PRESETS, validateCreateForm } from '../lib/presets';
import { nextLinkHref, RECIPES, recipeDocsHref, recipeForm, recipeKey, type Recipe, type RecipeId } from '../lib/recipes';
import { toast, useUi } from '../store/ui';
import { InlineError, Modal, ModeBadge, Switch } from '../components/ui';
import './create.css';
import { ModesLegend } from '../components/ChannelsTable';

const CHOICES: { mode: ChannelMode; icon: typeof Repeat; recommended?: boolean; example: string }[] = [
  { mode: 'stream', icon: Repeat, recommended: true, example: 'orders.events, audit.logs, clickstream' },
  { mode: 'queue', icon: ListOrdered, example: 'email, image-resize, webhooks' },
  { mode: 'keyed', icon: KeyRound, example: 'payments.events · customer_id' },
  { mode: 'priority', icon: ArrowUpWideNarrow, example: 'support.tickets, alerts.dispatch' },
];

/** Default partitions per mode (a priority channel's partitions are its levels). */
const defaultPartitions = (m: ChannelMode) => (m === 'queue' ? '1' : m === 'priority' ? '3' : '8');

const RECIPE_ICON: Record<RecipeId, typeof Repeat> = {
  'orders-keyed': KeyRound,
  'jobs-queue': ListChecks,
  'event-log': ScrollText,
  fanout: Share2,
  'content-routing': Route,
  'webhook-push': Webhook,
  'retry-poison': RotateCcw,
  'telemetry-ingest': Activity,
};

/** The channel just created from a recipe: its next steps stay on screen until dismissed. */
const useRecipeDone = create<{ done: { recipe: Recipe; name: string } | null; set: (d: { recipe: Recipe; name: string } | null) => void }>()((set) => ({
  done: null,
  set: (done) => set({ done }),
}));

export function CreateChannelWizard() {
  const open = useUi((s) => s.createOpen);
  const setOpen = useUi((s) => s.setCreateOpen);
  // Mounted only while open, so every open starts from a clean form.
  return (
    <>
      {open && <Wizard onClose={() => setOpen(false)} />}
      <RecipeNextSteps />
    </>
  );
}

function RecipeNextSteps() {
  const t = useT();
  const done = useRecipeDone((s) => s.done);
  const setDone = useRecipeDone((s) => s.set);
  const setCreateOpen = useUi((s) => s.setCreateOpen);
  if (!done) return null;
  const { recipe, name } = done;
  const close = () => setDone(null);
  return (
    <aside className="recipe-next" role="status" aria-label={t('recipes.next.aria')}>
      <div className="rn-head">
        <span className="rn-title">{t('recipes.next.title', { name })}</span>
        <button className="btn btn-ghost btn-icon btn-sm" onClick={close} aria-label={t('common.close')}>
          <X size={14} />
        </button>
      </div>
      <p className="rn-text">{t(recipeKey(recipe.id, 'next'))}</p>
      <div className="rn-links">
        {recipe.next.map((l) => {
          const href = nextLinkHref(l, name);
          return href ? (
            <Link key={l} to={href} onClick={close}>
              {t(`recipes.link.${l}`)}
            </Link>
          ) : (
            <button
              key={l}
              type="button"
              className="rn-linkbtn"
              onClick={() => {
                close();
                setCreateOpen(true);
              }}
            >
              {t(`recipes.link.${l}`)}
            </button>
          );
        })}
        <Link to={recipeDocsHref(recipe.id)} onClick={close}>
          {t('recipes.next.recipe')}
        </Link>
      </div>
    </aside>
  );
}

function Wizard({ onClose }: { onClose: () => void }) {
  const setOpen = (v: boolean) => {
    if (!v) onClose();
  };
  const t = useT();
  const navigate = useNavigate();
  const [step, setStep] = useState<1 | 2>(1);
  const [mode, setMode] = useState<ChannelMode>('stream');
  const [name, setName] = useState('');
  const [touched, setTouched] = useState(false);
  const [retention, setRetention] = useState<number | null>(7 * 86_400);
  const [partitions, setPartitions] = useState('8');
  const [orderingKey, setOrderingKey] = useState('');
  const [advanced, setAdvanced] = useState(false);
  const [attempts, setAttempts] = useState(String(DEFAULT_RETRY.max_attempts));
  const [backoff, setBackoff] = useState(formatBackoffList(DEFAULT_RETRY.backoff_ms));
  const [ackTimeout, setAckTimeout] = useState(String(DEFAULT_RETRY.ack_timeout_ms / 1000));
  const [dlq, setDlq] = useState(true);
  const [maxBytes, setMaxBytes] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const [serverError, setServerError] = useState<{ field: 'name' | null; msg: string } | null>(null);
  const [recipe, setRecipe] = useState<Recipe | null>(null);
  const setDone = useRecipeDone((s) => s.set);
  const bodyRef = useRef<HTMLDivElement>(null);
  const focusName = useRef(false);

  // Each step starts at the top of the modal body (step 1 may have been scrolled down to a card);
  // after a template pick the name field takes focus, since the clicked card is gone.
  useEffect(() => {
    bodyRef.current?.closest('.modal-body')?.scrollTo(0, 0);
    if (step === 2 && focusName.current) {
      focusName.current = false;
      document.getElementById('c-name')?.focus({ preventScroll: true });
    }
  }, [step]);

  /** Mode defaults (also what "Reset" on a template restores). */
  const applyDefaults = (m: ChannelMode) => {
    setPartitions(defaultPartitions(m));
    setRetention(7 * 86_400);
    setMaxBytes(null);
    setOrderingKey('');
    setAttempts(String(DEFAULT_RETRY.max_attempts));
    setBackoff(formatBackoffList(DEFAULT_RETRY.backoff_ms));
    setAckTimeout(String(DEFAULT_RETRY.ack_timeout_ms / 1000));
    setDlq(true);
  };

  const pickMode = (m: ChannelMode) => {
    setMode(m);
    setPartitions(defaultPartitions(m));
    setRecipe(null);
  };

  const pickRecipe = (r: Recipe) => {
    const f = recipeForm(r);
    setRecipe(r);
    setMode(r.mode);
    setName(f.name);
    setPartitions(f.partitions);
    setRetention(r.maxAgeSecs);
    setMaxBytes(r.maxBytes);
    setOrderingKey(r.orderingKey ?? '');
    setAttempts(f.attempts);
    setBackoff(f.backoff);
    setAckTimeout(f.ackTimeout);
    setDlq(r.dlq);
    setServerError(null);
    setTouched(false);
    focusName.current = true;
    setStep(2);
  };

  const resetRecipe = () => {
    if (recipe && name === recipe.suggestedName) setName('');
    applyDefaults(mode);
    setRecipe(null);
  };

  const v = validateCreateForm({ name, partitions, attempts, backoff, ackTimeout });
  const nameError = v.name;
  const partitionsN = Number(partitions);
  const levelsBad = mode === 'priority' && !(partitionsN >= PRIORITY_LEVELS.min && partitionsN <= PRIORITY_LEVELS.max && Number.isInteger(partitionsN));
  const partitionsError = levelsBad ? t('channels.create.levelsRange', { min: PRIORITY_LEVELS.min, max: PRIORITY_LEVELS.max }) : v.partitions ? t('channels.create.partitionsRange') : null;
  const backoffMs = v.backoffMs;
  const attemptsN = Number(attempts);
  const ackN = Number(ackTimeout);
  const advError = v.advanced;
  const canSubmit = !nameError && !partitionsError && !advError && !busy;

  const submit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    setTouched(true);
    if (!canSubmit || !backoffMs) {
      if (advError) setAdvanced(true);
      return;
    }
    setBusy(true);
    setServerError(null);
    try {
      const cfg = await api.createChannel({
        name,
        mode,
        partitions: partitionsN,
        retention: { max_age_secs: retention, max_bytes: maxBytes },
        retry: { max_attempts: attemptsN, backoff_ms: backoffMs, ack_timeout_ms: Math.round(ackN * 1000) },
        dlq_enabled: dlq,
        ordering_key: orderingKey.trim() || null,
      });
      // From a template, the next-steps card says "created" itself; otherwise a plain toast.
      if (recipe) setDone({ recipe, name: cfg.name });
      else toast('success', t('channels.create.created', { name: cfg.name }));
      setOpen(false);
      navigate(channelPath(cfg.name));
    } catch (err) {
      const conflict = err instanceof ApiError && err.status === 409;
      setServerError({ field: conflict || (err instanceof ApiError && err.status === 400) ? 'name' : null, msg: conflict ? t('channels.create.exists', { name }) : errorMessage(err) });
    } finally {
      setBusy(false);
    }
  };

  const choice = CHOICES.find((c) => c.mode === mode)!;

  return (
    <Modal
      open
      onClose={onClose}
      wide
      title={step === 1 ? t('channels.create.step1Title') : t('channels.create.step2Title', { mode: MODE_LABEL[mode].toLowerCase() })}
      description={step === 1 ? t('channels.create.step1Desc') : t(`channels.create.choice.${choice.mode}.title`)}
      footer={
        step === 1 ? (
          <>
            <span className="faint" style={{ marginRight: 'auto', fontSize: 12 }}>
              {t('channels.create.stepOf')}
            </span>
            <button className="btn" onClick={() => setOpen(false)}>
              {t('common.cancel')}
            </button>
            <button className="btn btn-primary" onClick={() => setStep(2)} data-autofocus>
              {t('channels.create.continue')} <ArrowRight size={14} />
            </button>
          </>
        ) : (
          <>
            <button className="btn btn-ghost" style={{ marginRight: 'auto' }} onClick={() => setStep(1)}>
              <ArrowLeft size={14} /> {t('common.back')}
            </button>
            <button className="btn" onClick={() => setOpen(false)}>
              {t('common.cancel')}
            </button>
            <button className="btn btn-primary" onClick={() => submit()} disabled={busy || (touched && !canSubmit)}>
              {busy ? t('common.creating') : t('channels.create')}
            </button>
          </>
        )
      }
    >
      <div ref={bodyRef} hidden />
      {step === 1 ? (
        <>
        <section className="recipes" aria-labelledby="recipes-title">
          <div className="recipes-head">
            <h3 id="recipes-title">
              <LayoutTemplate size={14} aria-hidden /> {t('recipes.section.title')}
            </h3>
            <Link to="/docs/recipes" className="recipes-all" onClick={onClose}>
              {t('recipes.section.all')} <ArrowUpRight size={12} aria-hidden />
            </Link>
          </div>
          <p className="recipes-desc faint">{t('recipes.section.desc')}</p>
          <ul className="recipe-grid">
            {RECIPES.map((r) => {
              const Icon = RECIPE_ICON[r.id];
              const title = t(recipeKey(r.id, 'title'));
              return (
                <li key={r.id} className={`recipe-card ${recipe?.id === r.id ? 'selected' : ''}`}>
                  <button type="button" className="rc-main" onClick={() => pickRecipe(r)} aria-describedby={`rc-sum-${r.id}`}>
                    <span className="rc-icon" aria-hidden>
                      <Icon size={16} />
                    </span>
                    <span className="rc-text">
                      <span className="rc-title">{title}</span>
                      <span id={`rc-sum-${r.id}`} className="rc-sum">
                        {t(recipeKey(r.id, 'summary'))}
                      </span>
                    </span>
                  </button>
                  <span className="rc-foot">
                    <ModeBadge mode={r.mode} />
                    <Link to={recipeDocsHref(r.id)} className="rc-more" onClick={onClose} aria-label={t('recipes.moreAria', { title })}>
                      {t('recipes.more')}
                    </Link>
                  </span>
                </li>
              );
            })}
          </ul>
        </section>
        <h3 className="recipes-or">{t('recipes.orPick')}</h3>
        <ModesLegend className="mode-legend-hint faint" />
        <div className="mode-choices" role="radiogroup" aria-label={t('channels.create.modeAria')}>
          {CHOICES.map((c) => (
            <label key={c.mode} className={`mode-choice ${mode === c.mode ? 'selected' : ''}`}>
              <input type="radio" name="mode" value={c.mode} checked={mode === c.mode} onChange={() => pickMode(c.mode)} />
              <span className="mc-icon" aria-hidden>
                <c.icon size={18} />
              </span>
              <span className="mc-text">
                <span className="mc-title">
                  {t(`channels.create.choice.${c.mode}.title`)}
                  {c.recommended && <span className="badge badge-primary">{t('channels.create.recommended')}</span>}
                </span>
                <span className="mc-desc">{t(`channels.create.choice.${c.mode}.desc`)}</span>
                <span className="mc-example">
                  <b>{MODE_LABEL[c.mode].toUpperCase()}</b> · {t('channels.create.example', { example: c.example })}
                </span>
              </span>
            </label>
          ))}
        </div>
        </>
      ) : (
        <form className="create-form" onSubmit={submit}>
          {recipe && (
            <div className="recipe-chip">
              <LayoutTemplate size={13} aria-hidden />
              <span className="rchip-label">{t('recipes.chip', { title: t(recipeKey(recipe.id, 'title')) })}</span>
              <span className="rchip-actions">
                <span className="faint" aria-hidden>
                  ·
                </span>
                <button type="button" className="rchip-reset" onClick={resetRecipe} aria-label={t('recipes.resetAria')}>
                  {t('recipes.reset')}
                </button>
                {/* A new tab: the form keeps what was typed. */}
                <Link to={recipeDocsHref(recipe.id)} className="rchip-more" target="_blank" rel="noopener">
                  {t('recipes.more')} <ArrowUpRight size={12} aria-hidden />
                </Link>
              </span>
            </div>
          )}
          <div className="field">
            <label htmlFor="c-name">{t('common.name')}</label>
            <input
              id="c-name"
              className={`input mono ${(touched || name) && (nameError || serverError?.field === 'name') ? 'invalid' : ''}`}
              placeholder={mode === 'queue' ? 'email' : mode === 'keyed' ? 'payments.events' : 'orders.events'}
              value={name}
              onChange={(e) => {
                setName(e.target.value);
                setServerError(null);
              }}
              onBlur={() => setTouched(true)}
              autoComplete="off"
              spellCheck={false}
              data-autofocus
              aria-describedby="c-name-hint"
            />
            <span id="c-name-hint" className={(touched || name) && (nameError || serverError?.field === 'name') ? 'field-error' : 'field-hint'}>
              {(touched || name) && nameError ? nameError : serverError?.field === 'name' ? serverError.msg : t('channels.create.nameHint')}
            </span>
          </div>

          <div className="field">
            <span className="field-label">{t('channels.create.keepFor')}</span>
            <div className="chip-row" role="radiogroup" aria-label={t('common.retention')}>
              {RETENTION_PRESETS.map((p) => (
                <button type="button" key={String(p.secs)} role="radio" aria-checked={retention === p.secs} className={`chip ${retention === p.secs ? 'on' : ''}`} onClick={() => setRetention(p.secs)}>
                  {p.label}
                </button>
              ))}
            </div>
            {mode === 'queue' && <span className="field-hint">{t('channels.create.queueRetentionHint')}</span>}
          </div>

          <div className="form-row">
            <div className="field">
              <label htmlFor="c-parts">{mode === 'priority' ? t('channels.create.levelsLabel') : t('channels.create.partitionsLabel')}</label>
              <input id="c-parts" className={`input ${partitionsError ? 'invalid' : ''}`} inputMode="numeric" value={partitions} onChange={(e) => setPartitions(e.target.value)} />
              <span className={partitionsError ? 'field-error' : 'field-hint'}>{partitionsError ?? (mode === 'queue' ? t('channels.create.partitionsQueueHint') : mode === 'priority' ? t('channels.create.levelsHint') : t('channels.create.partitionsHint'))}</span>
            </div>
            <div className="field">
              <label htmlFor="c-key">
                {t('channels.orderingKey')} {mode !== 'keyed' && <span className="faint">({t('common.optional')})</span>}
              </label>
              <input id="c-key" className="input mono" placeholder={mode === 'keyed' ? 'customer_id' : 'order_id'} value={orderingKey} onChange={(e) => setOrderingKey(e.target.value)} />
              <span className="field-hint">{mode === 'keyed' ? t('channels.create.keyHintKeyed') : t('channels.create.keyHint')}</span>
            </div>
          </div>

          <button type="button" className="adv-toggle" onClick={() => setAdvanced(!advanced)} aria-expanded={advanced}>
            {advanced ? <ChevronDown size={14} /> : <ChevronRight size={14} />} {t('channels.create.advanced')}
            <span className="faint">
              {' '}
              {t('channels.create.advancedSummary', { attempts, backoff: backoff || '…', dlq: dlq ? t('common.on') : t('common.off') })}
            </span>
          </button>
          {advanced && (
            <div className="adv-panel">
              <div className="form-row">
                <div className="field">
                  <label htmlFor="c-att">{t('channels.retry.maxAttempts')}</label>
                  <input id="c-att" className={`input ${advError === 'attempts' ? 'invalid' : ''}`} inputMode="numeric" value={attempts} onChange={(e) => setAttempts(e.target.value)} />
                </div>
                <div className="field">
                  <label htmlFor="c-ack">{t('channels.retry.ackTimeoutSecs')}</label>
                  <input id="c-ack" className={`input ${advError === 'ack' ? 'invalid' : ''}`} inputMode="decimal" value={ackTimeout} onChange={(e) => setAckTimeout(e.target.value)} />
                </div>
              </div>
              <div className="field">
                <label htmlFor="c-backoff">{t('channels.retry.backoff')}</label>
                <input id="c-backoff" className={`input mono ${advError === 'backoff' ? 'invalid' : ''}`} value={backoff} onChange={(e) => setBackoff(e.target.value)} />
                <span className={advError === 'backoff' ? 'field-error' : 'field-hint'}>{t('channels.retry.backoffHint')}</span>
              </div>
              <div className="form-row">
                <div className="field">
                  <span className="field-label">{t('channels.dlqFull')}</span>
                  <div className="switch-row">
                    <Switch checked={dlq} onChange={setDlq} label={t('channels.dlqFull')} />
                    <span className="muted">{dlq ? t('channels.dlqOn') : t('channels.dlqOff')}</span>
                  </div>
                </div>
                <div className="field">
                  <label htmlFor="c-max">{t('channels.sizeLimit')}</label>
                  <select id="c-max" className="select" value={String(maxBytes)} onChange={(e) => setMaxBytes(e.target.value === 'null' ? null : Number(e.target.value))}>
                    {MAX_BYTES_PRESETS.map((p) => (
                      <option key={String(p.bytes)} value={String(p.bytes)}>
                        {p.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
            </div>
          )}
          {serverError && serverError.field == null && <InlineError>{serverError.msg}</InlineError>}
          <button type="submit" hidden />
        </form>
      )}
    </Modal>
  );
}

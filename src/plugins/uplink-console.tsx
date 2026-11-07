/**
 * Send Uplink Console.
 *
 * A live, state-aware control panel for the rocket's uplink command set. The
 * layout and command grouping mirror the previous ground-station CLI's "Send
 * Uplink" panel (`prev-rocket-cli/src/gs/mod.rs`): a vertical list of labelled
 * controls in the same order, with confirm-before-send, a "Sending…" state, and
 * an ACK / error-with-retry result.
 *
 * Each control emits a typed {@link UplinkCommand} (mirroring the firmware
 * `VLPUplinkPacket` variants) rather than a free-form string, so commands are
 * grouped exactly as the firmware models them:
 *
 *   - Target Apogee → one `SetTargetApogee`;
 *   - the five modes → `ChangeMode` (a one-of, not five toggles);
 *   - Reset → a single device picker → one `Reset`;
 *   - Overwrite AMP → one `AMPOutputOverwrite` carrying all four outputs;
 *   - Overwrite EPS → one `PayloadEPSOutputOverwrite` carrying all six rails;
 *   - Fire Main / Drogue Pyro → `FirePyro`.
 *
 * Config (frequency / power) is *not* an uplink packet — in the CLI it edits
 * local ground-station radio config applied on restart — so it renders as a
 * separate local-config section that does not transmit.
 *
 * Current-state badges are sourced from the *downlink* (`gsd_*`) signals, which
 * are the real readback for these commands; commands with no downlink mirror
 * (Target Apogee, Reset) simply show no current value, as the CLI does.
 *
 */
import { useState, useEffect, useCallback } from 'react'
import { NAMESPACE } from './data-provider'
import type { DataKey, Enumeration } from './data-provider'
import {
  BOOL_ENUM,
  FLIGHT_STAGE_ENUM,
  POWER_OUTPUT_STATUS_ENUM,
  SEND_UPLINK,
  TELEMETRY_HOST_URL,
} from '../sources/rocket-telemetry'
import type { TreeNode } from '../sources/rocket-telemetry'
import {
  MODES,
  RESET_DEVICES,
  OVERWRITE_OPTIONS,
  NO_OVERWRITE,
  summarize,
  isDestructive,
} from './uplink-command'
import type {
  UplinkCommand,
  Mode,
  DeviceToReset,
  PowerOutputOverwrite,
  AMPOverwrite,
  EPSOverwrite,
} from './uplink-command'
import { mountReactInShadow } from './react-utils'
import { SectionLabel, ActionButton } from './ui'
import type { OpenMCT } from 'openmct'

export const UPLINK_CONSOLE_NAMESPACE = 'uplink-console'
export const UPLINK_CONSOLE_TYPE = `${NAMESPACE}.uplink-console`
const UPLINK_CONSOLE_ROOT_KEY = 'console'

// ---------------------------------------------------------------------------
// Signal lookup & formatting
// ---------------------------------------------------------------------------

/** Every signal under Send Uplink, flattened, for numeric constraint lookup. */
function flattenSignals(node: TreeNode): DataKey[] {
  return node.kind === 'signal'
    ? [node.signal]
    : node.children.flatMap(flattenSignals)
}
const SIGNAL_BY_KEY = new Map(
  flattenSignals(SEND_UPLINK).map((s) => [s.key, s])
)
const signal = (key: string): DataKey =>
  SIGNAL_BY_KEY.get(key) ?? { key, gapThreshold: 0 }

/** Decimal places declared by a `%.Nf` format string, if any. */
function decimalPlaces(s: DataKey): number | undefined {
  const match = s.format?.match(/%\.(\d+)f/)
  return match ? Number(match[1]) : undefined
}

/** Bare numeric string for seeding an editable input (no unit). */
function inputValue(s: DataKey, value: number): string {
  const dp = decimalPlaces(s)
  return dp !== undefined ? value.toFixed(dp) : String(value)
}

/** Look up an enum label for a numeric value. */
function enumLabel(table: Enumeration[], value: number | undefined): string {
  if (value === undefined || Number.isNaN(value)) return '—'
  return table.find((e) => e.value === value)?.string ?? String(value)
}

// ---------------------------------------------------------------------------
// Command → downlink state mapping
// ---------------------------------------------------------------------------

type AmpField = keyof AMPOverwrite
type EpsField = keyof EPSOverwrite

/** AMP outputs: staged overwrite field + the downlink keys that read it back. */
const AMP_OUTPUTS: {
  field: AmpField
  label: string
  statusKey: string
  overwroteKey: string
}[] = [
  {
    field: 'out1',
    label: 'Out 1',
    statusKey: 'gsd_amp_out1',
    overwroteKey: 'gsd_amp_out1_overwrote',
  },
  {
    field: 'out2',
    label: 'Out 2',
    statusKey: 'gsd_amp_out2',
    overwroteKey: 'gsd_amp_out2_overwrote',
  },
  {
    field: 'out3',
    label: 'Out 3',
    statusKey: 'gsd_amp_out3',
    overwroteKey: 'gsd_amp_out3_overwrote',
  },
  {
    field: 'out4',
    label: 'Out 4',
    statusKey: 'gsd_amp_out4',
    overwroteKey: 'gsd_amp_out4_overwrote',
  },
]

/** EPS rails: staged overwrite field + the downlink keys that read it back. */
const EPS_RAILS: {
  field: EpsField
  label: string
  statusKey: string
  overwroteKey: string
}[] = [
  {
    field: 'eps1_3v3',
    label: 'EPS 1 · 3.3V',
    statusKey: 'gsd_eps1_output_3v3_status',
    overwroteKey: 'gsd_eps1_output_3v3_overwrote',
  },
  {
    field: 'eps1_5v',
    label: 'EPS 1 · 5V',
    statusKey: 'gsd_eps1_output_5v_status',
    overwroteKey: 'gsd_eps1_output_5v_overwrote',
  },
  {
    field: 'eps1_9v',
    label: 'EPS 1 · 9V',
    statusKey: 'gsd_eps1_output_9v_status',
    overwroteKey: 'gsd_eps1_output_9v_overwrote',
  },
  {
    field: 'eps2_3v3',
    label: 'EPS 2 · 3.3V',
    statusKey: 'gsd_eps2_output_3v3_status',
    overwroteKey: 'gsd_eps2_output_3v3_overwrote',
  },
  {
    field: 'eps2_5v',
    label: 'EPS 2 · 5V',
    statusKey: 'gsd_eps2_output_5v_status',
    overwroteKey: 'gsd_eps2_output_5v_overwrote',
  },
  {
    field: 'eps2_9v',
    label: 'EPS 2 · 9V',
    statusKey: 'gsd_eps2_output_9v_status',
    overwroteKey: 'gsd_eps2_output_9v_overwrote',
  },
]

/** Flight stages that correspond to a commandable mode (for button highlight). */
const STAGE_TO_MODE: Record<number, Mode> = {
  0: 'LowPower',
  1: 'SelfTest',
  2: 'Armed',
  7: 'Landed',
}

/** Downlink + config keys the console subscribes to for live state. */
const LIVE_KEYS: string[] = [
  'gsd_flight_stage',
  ...AMP_OUTPUTS.flatMap((o) => [o.statusKey, o.overwroteKey]),
  ...EPS_RAILS.flatMap((r) => [r.statusKey, r.overwroteKey]),
  'gsd_pyro_main_continuity',
  'gsd_pyro_drogue_continuity',
  'uplink_config_frequency',
  'uplink_config_power',
]

/** Power-output status text, noting when an overwrite is in effect. */
function powerStatusText(
  status: number | undefined,
  overwrote: number | undefined
): string {
  const text = enumLabel(POWER_OUTPUT_STATUS_ENUM, status)
  return overwrote === 1 ? `${text} · overwritten` : text
}

// ---------------------------------------------------------------------------
// Simulated and real uplink transport (mirrors uplink-panel.tsx)
// ---------------------------------------------------------------------------

type Ack = { rssi: number; snr: number }

/**
 * Sends the real uplink, same interface as sendTestUplink
 */
export async function sendUplink(command: UplinkCommand): Promise<Ack> {
  console.debug('[uplink] sending', command, '—', summarize(command));

  let response: Response;
  try {
    response = await fetch('http://' + TELEMETRY_HOST_URL + '/cmd', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      body: JSON.stringify(command),
    });
  } catch (networkError) {
    throw new Error(
      `Network error sending uplink: ${
        networkError instanceof Error ? networkError.message : String(networkError)
      }`
    );
  }

  if (!response.ok) {
    const errorBody = await response.text().catch(() => '');
    throw new Error(
      `No ACK received [HTTP ${response.status} ${response.statusText}]: ${
        errorBody || 'Server rejected command'
      }`
    );
  }

  try {
    const ack: Ack = await response.json();
    return ack;
  } catch (parseError) {
    throw new Error('Failed to parse ACK payload from server');
  }
}
/**
 * Stand-in for the ground-station uplink transmit. Fakes latency, an occasional
 * link failure, and an ACK RSSI/SNR. Layer B replaces this with real packet
 * serialization + transmit; keep the resolve({@link Ack}) / reject(Error)
 * contract.
 */
function sendTestUplink(command: UplinkCommand): Promise<Ack> {
  console.debug('[uplink] sending', command, '—', summarize(command))
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      if (Math.random() < 0.2) {
        reject(new Error('No ACK received (link timeout)'))
        return
      }
      resolve({
        rssi: -90 - Math.round(Math.random() * 25),
        snr: 4 + Math.round(Math.random() * 8),
      })
    }, 700)
  })
}

type Feedback =
  | { status: 'idle' }
  | { status: 'sending'; label: string }
  | { status: 'success'; label: string; ack: Ack }
  | { status: 'error'; label: string; message: string }

// ---------------------------------------------------------------------------
// Live values hook
// ---------------------------------------------------------------------------

/** A live datum as delivered by the NICK telemetry provider. */
type LiveDatum = { value: number }

/**
 * Subscribe to a fixed set of telemetry keys, seeding each with its latest
 * value and tracking realtime updates. Returns the latest value per key.
 */
function useLiveValues(
  openmct: OpenMCT,
  keys: string[]
): Record<string, number | undefined> {
  const [values, setValues] = useState<Record<string, number | undefined>>({})

  useEffect(() => {
    const unsubscribers: Array<() => void> = []
    let cancelled = false

    const setValue = (key: string, value: number) =>
      setValues((prev) => ({ ...prev, [key]: value }))

    for (const key of keys) {
      const identifier = { namespace: NAMESPACE, key }
      openmct.objects.get(identifier).then((domainObject) => {
        if (cancelled || !domainObject) return

        openmct.telemetry
          .request(domainObject, { size: 1, strategy: 'latest' })
          .then((data: LiveDatum[]) => {
            const latest = data?.[data.length - 1]
            if (latest && !cancelled) setValue(key, latest.value)
          })
          .catch(() => {})

        const unsubscribe = openmct.telemetry.subscribe(
          domainObject,
          (datum: LiveDatum) => setValue(key, datum.value)
        )
        if (cancelled) unsubscribe()
        else unsubscribers.push(unsubscribe)
      })
    }

    return () => {
      cancelled = true
      for (const unsubscribe of unsubscribers) unsubscribe()
    }
  }, [openmct, keys])

  return values
}

// ---------------------------------------------------------------------------
// Shared styling
// ---------------------------------------------------------------------------

const LABEL = 'w-[150px] shrink-0 text-[11px] text-gray-600 dark:text-gray-300'
const NOW =
  'min-w-[120px] text-[11px] tabular-nums text-gray-400 dark:text-gray-500'
const ROW = 'flex items-center gap-2'

// ---------------------------------------------------------------------------
// Controls
// ---------------------------------------------------------------------------

/** A 3-way No-Overwrite / Enable / Disable selector (one AMP/EPS field). */
function OverwriteSelector({
  value,
  disabled,
  onChange,
}: {
  value: PowerOutputOverwrite
  disabled: boolean
  onChange: (value: PowerOutputOverwrite) => void
}) {
  return (
    <div className="flex gap-1">
      {OVERWRITE_OPTIONS.map((opt) => {
        const active = value === opt.value
        return (
          <button
            key={opt.value}
            disabled={disabled}
            onClick={() => onChange(opt.value)}
            className={[
              'rounded px-2 py-0.5 text-[10px] border transition-colors cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed',
              active
                ? 'bg-blue-600 text-white border-blue-600 font-semibold'
                : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 border-gray-300 dark:border-gray-600 hover:border-blue-500',
            ].join(' ')}
          >
            {opt.label}
          </button>
        )
      })}
    </div>
  )
}

/** Local ground-station radio config (frequency / power) — does NOT transmit. */
function ConfigControl({
  freqValue,
  powerValue,
}: {
  freqValue: number | undefined
  powerValue: number | undefined
}) {
  const freq = signal('uplink_config_frequency')
  const power = signal('uplink_config_power')
  const [freqEdit, setFreqEdit] = useState<string | null>(null)
  const [powerEdit, setPowerEdit] = useState<string | null>(null)
  const [note, setNote] = useState<string | null>(null)

  const freqText =
    freqEdit ?? (freqValue !== undefined ? inputValue(freq, freqValue) : '')
  const powerText =
    powerEdit ?? (powerValue !== undefined ? inputValue(power, powerValue) : '')

  const inRange = (s: DataKey, n: number) =>
    !((s.min !== undefined && n < s.min) || (s.max !== undefined && n > s.max))

  const save = () => {
    const f = Number(freqText)
    const p = Number(powerText)
    if (!Number.isFinite(f) || !inRange(freq, f)) {
      alert(
        `Frequency must be between ${freq.min} and ${freq.max} ${freq.unit}.`
      )
      return
    }
    if (!Number.isFinite(p) || !inRange(power, p)) {
      alert(
        `Power must be between ${power.min} and ${power.max} ${power.unit}.`
      )
      return
    }
    // No transmit: this is local radio config (CLI saves it to disk and applies
    // on restart). NICK has no config store yet, so just acknowledge locally.
    setFreqEdit(null)
    setPowerEdit(null)
    setNote(
      `Saved locally — ${f} ${freq.unit}, ${p} ${power.unit}. Applies on restart.`
    )
  }

  return (
    <div>
      <SectionLabel>Config (local radio)</SectionLabel>
      <div className="flex flex-col gap-2">
        <div className={ROW}>
          <span className={LABEL}>Frequency</span>
          <input
            type="number"
            value={freqText}
            min={freq.min}
            max={freq.max}
            onChange={(e) => setFreqEdit(e.target.value)}
            className="w-[100px] rounded border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-800 text-gray-800 dark:text-gray-100 text-[11px] px-2 py-1 outline-none focus:border-blue-500"
          />
          <span className="text-[11px] text-gray-400 dark:text-gray-500">
            {freq.unit}
          </span>
        </div>
        <div className={ROW}>
          <span className={LABEL}>Power</span>
          <input
            type="number"
            value={powerText}
            min={power.min}
            max={power.max}
            onChange={(e) => setPowerEdit(e.target.value)}
            className="w-[100px] rounded border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-800 text-gray-800 dark:text-gray-100 text-[11px] px-2 py-1 outline-none focus:border-blue-500"
          />
          <span className="text-[11px] text-gray-400 dark:text-gray-500">
            {power.unit}
          </span>
          <ActionButton onClick={save}>Save Config</ActionButton>
        </div>
        {note && (
          <p className="text-[11px] text-gray-400 dark:text-gray-500">{note}</p>
        )}
      </div>
    </div>
  )
}

/** Editable numeric input + Send (Target Apogee). */
function NumericControl({
  signalKey,
  disabled,
  onSend,
}: {
  signalKey: string
  disabled: boolean
  onSend: (value: number) => void
}) {
  const s = signal(signalKey)
  const [text, setText] = useState('')

  const submit = () => {
    const n = Number(text)
    if (!Number.isFinite(n) || text.trim() === '') {
      alert(`${s.name}: enter a valid number.`)
      return
    }
    if (
      (s.min !== undefined && n < s.min) ||
      (s.max !== undefined && n > s.max)
    ) {
      alert(`${s.name} must be between ${s.min} and ${s.max}.`)
      return
    }
    onSend(n)
  }

  return (
    <div className={ROW}>
      <span className={LABEL}>{s.name}</span>
      <input
        type="number"
        value={text}
        min={s.min}
        max={s.max}
        placeholder={s.max !== undefined ? `0–${s.max}` : ''}
        onChange={(e) => setText(e.target.value)}
        className="w-[100px] rounded border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-800 text-gray-800 dark:text-gray-100 text-[11px] px-2 py-1 outline-none focus:border-blue-500"
      />
      {s.unit && (
        <span className="text-[11px] text-gray-400 dark:text-gray-500">
          {s.unit}
        </span>
      )}
      <ActionButton variant="primary" disabled={disabled} onClick={submit}>
        Send
      </ActionButton>
    </div>
  )
}

/** The five modes as a one-of, with the current flight stage highlighted. */
function ModeControl({
  stage,
  disabled,
  onSend,
}: {
  stage: number | undefined
  disabled: boolean
  onSend: (mode: Mode) => void
}) {
  const currentMode = stage !== undefined ? STAGE_TO_MODE[stage] : undefined
  return (
    <div>
      <div className={ROW + ' mb-1.5'}>
        <span className={LABEL}>Mode</span>
        <span className={NOW}>
          Stage: {enumLabel(FLIGHT_STAGE_ENUM, stage)}
        </span>
      </div>
      <div className="flex gap-1 flex-wrap pl-[150px]">
        {MODES.map((m) => {
          const active = currentMode === m.mode
          return (
            <button
              key={m.mode}
              disabled={disabled}
              onClick={() => onSend(m.mode)}
              className={[
                'rounded px-2 py-1 text-[11px] border transition-colors cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed',
                active
                  ? 'bg-blue-600 text-white border-blue-600 font-semibold'
                  : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 border-gray-300 dark:border-gray-600 hover:border-blue-500 hover:text-blue-600 dark:hover:text-blue-400',
              ].join(' ')}
            >
              {m.label}
            </button>
          )
        })}
      </div>
    </div>
  )
}

/** Reset device picker → single Reset command. */
function ResetControl({
  disabled,
  onSend,
}: {
  disabled: boolean
  onSend: (device: DeviceToReset) => void
}) {
  const [device, setDevice] = useState<DeviceToReset>(RESET_DEVICES[0].device)
  return (
    <div className={ROW}>
      <span className={LABEL}>Reset Device</span>
      <select
        value={device}
        disabled={disabled}
        onChange={(e) => setDevice(e.target.value as DeviceToReset)}
        className="w-[200px] rounded border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-800 text-gray-800 dark:text-gray-100 text-[11px] px-2 py-1 outline-none focus:border-blue-500 disabled:opacity-50"
      >
        {RESET_DEVICES.map((d) => (
          <option key={d.device} value={d.device}>
            {d.label}
          </option>
        ))}
      </select>
      <ActionButton
        variant="primary"
        disabled={disabled}
        onClick={() => onSend(device)}
      >
        Reset
      </ActionButton>
    </div>
  )
}

/**
 * A batched overwrite form (AMP or EPS): one row per output/rail with its live
 * downlink status and a staged selector, plus a single Send building one packet.
 */
function OverwriteForm<F extends string>({
  title,
  rows,
  values,
  disabled,
  onSend,
}: {
  title: string
  rows: { field: F; label: string; statusKey: string; overwroteKey: string }[]
  values: Record<string, number | undefined>
  disabled: boolean
  onSend: (staged: Record<F, PowerOutputOverwrite>) => void
}) {
  const initial = () =>
    Object.fromEntries(rows.map((r) => [r.field, NO_OVERWRITE])) as Record<
      F,
      PowerOutputOverwrite
    >
  const [staged, setStaged] = useState<Record<F, PowerOutputOverwrite>>(initial)
  const set = (field: F, value: PowerOutputOverwrite) =>
    setStaged((prev) => ({ ...prev, [field]: value }))

  return (
    <div>
      <SectionLabel>{title}</SectionLabel>
      <div className="flex flex-col gap-1.5">
        {rows.map((r) => (
          <div key={r.field} className={ROW}>
            <span className={LABEL}>{r.label}</span>
            <span className={NOW}>
              {powerStatusText(values[r.statusKey], values[r.overwroteKey])}
            </span>
            <OverwriteSelector
              value={staged[r.field]}
              disabled={disabled}
              onChange={(v) => set(r.field, v)}
            />
          </div>
        ))}
        <div className="pl-[150px] pt-0.5">
          <ActionButton
            variant="primary"
            disabled={disabled}
            onClick={() => {
              onSend(staged)
              setStaged(initial())
            }}
          >
            Send {title}
          </ActionButton>
        </div>
      </div>
    </div>
  )
}

/** Fire Pyro: continuity badge + a destructive Fire button. */
function PyroControl({
  label,
  continuity,
  disabled,
  onFire,
}: {
  label: string
  continuity: number | undefined
  disabled: boolean
  onFire: () => void
}) {
  const hasContinuity = continuity === 1
  return (
    <div className={ROW}>
      <span className={LABEL}>{label}</span>
      <span
        className={[
          NOW,
          continuity === undefined
            ? ''
            : hasContinuity
              ? 'text-green-600 dark:text-green-400'
              : 'text-red-500',
        ].join(' ')}
      >
        Continuity: {enumLabel(BOOL_ENUM, continuity)}
      </span>
      <ActionButton variant="danger" disabled={disabled} onClick={onFire}>
        Fire
      </ActionButton>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Console view
// ---------------------------------------------------------------------------

function UplinkConsole({ openmct }: { openmct: OpenMCT }) {
  const values = useLiveValues(openmct, LIVE_KEYS)
  const [feedback, setFeedback] = useState<Feedback>({ status: 'idle' })
  const [lastCommand, setLastCommand] = useState<UplinkCommand | null>(null)
  const isSending = feedback.status === 'sending'

  const dispatch = useCallback((command: UplinkCommand) => {
    const summary = summarize(command)
    const prompt = isDestructive(command)
      ? `⚠ DESTRUCTIVE COMMAND ⚠\n\n${summary}\n\nThis cannot be undone. Send anyway?`
      : `Send uplink command?\n\n${summary}`
    if (!confirm(prompt)) return

    setLastCommand(command)
    setFeedback({ status: 'sending', label: summary })
    sendUplink(command)
      .then((ack) => setFeedback({ status: 'success', label: summary, ack }))
      .catch((err: unknown) =>
        setFeedback({
          status: 'error',
          label: summary,
          message: err instanceof Error ? err.message : String(err),
        })
      )
  }, [])

  const retry = useCallback(() => {
    if (lastCommand) dispatch(lastCommand)
  }, [dispatch, lastCommand])

  return (
    <div className="h-full overflow-y-auto text-gray-800 dark:text-gray-100 text-[12px] font-sans p-3.5">
      <p className="text-[13px] font-semibold mb-3">Send Uplink</p>

      <div className="flex flex-col gap-4">
        <ConfigControl
          freqValue={values['uplink_config_frequency']}
          powerValue={values['uplink_config_power']}
        />

        <NumericControl
          signalKey="uplink_target_apogee"
          disabled={isSending}
          onSend={(altitude) => dispatch({ type: 'SetTargetApogee', altitude })}
        />

        <ModeControl
          stage={values['gsd_flight_stage']}
          disabled={isSending}
          onSend={(mode) => dispatch({ type: 'ChangeMode', mode })}
        />

        <ResetControl
          disabled={isSending}
          onSend={(device) => dispatch({ type: 'Reset', device })}
        />

        <OverwriteForm<AmpField>
          title="Overwrite AMP"
          rows={AMP_OUTPUTS}
          values={values}
          disabled={isSending}
          onSend={(staged) =>
            dispatch({ type: 'AMPOutputOverwrite', ...staged })
          }
        />

        <OverwriteForm<EpsField>
          title="Overwrite EPS"
          rows={EPS_RAILS}
          values={values}
          disabled={isSending}
          onSend={(staged) =>
            dispatch({ type: 'PayloadEPSOutputOverwrite', ...staged })
          }
        />

        <div>
          <SectionLabel>Pyro</SectionLabel>
          <div className="flex flex-col gap-2">
            <PyroControl
              label="Fire Main Pyro"
              continuity={values['gsd_pyro_main_continuity']}
              disabled={isSending}
              onFire={() => dispatch({ type: 'FirePyro', pyro: 'PyroMain' })}
            />
            <PyroControl
              label="Fire Drogue Pyro"
              continuity={values['gsd_pyro_drogue_continuity']}
              disabled={isSending}
              onFire={() => dispatch({ type: 'FirePyro', pyro: 'PyroDrogue' })}
            />
          </div>
        </div>
      </div>

      {feedback.status !== 'idle' && (
        <div className="border-t border-gray-200 dark:border-gray-700 mt-4 pt-2.5">
          <SectionLabel>Last Command</SectionLabel>
          {feedback.status === 'sending' && (
            <p className="text-[11px] text-gray-400 dark:text-gray-500">
              Sending “{feedback.label}”…
            </p>
          )}
          {feedback.status === 'success' && (
            <p className="text-[11px] text-green-600 dark:text-green-400">
              ✓ “{feedback.label}” — ACK (RSSI {feedback.ack.rssi} dBm, SNR{' '}
              {feedback.ack.snr} dB)
            </p>
          )}
          {feedback.status === 'error' && (
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-[11px] text-red-500">
                ✗ “{feedback.label}” — {feedback.message}
              </span>
              <ActionButton disabled={isSending} onClick={retry}>
                Retry
              </ActionButton>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

export function UplinkConsolePlugin(openmct: OpenMCT) {
  openmct.types.addType(UPLINK_CONSOLE_TYPE, {
    name: 'Send Uplink Console',
    description:
      'Live state of every Send Uplink command, with editable inputs and command buttons',
    cssClass: 'icon-arrow-up',
  })

  openmct.objectViews.addProvider({
    key: `${UPLINK_CONSOLE_TYPE}.view`,
    name: 'Send Uplink Console',
    cssClass: 'icon-arrow-up',

    canView(domainObject: { type: string }) {
      return domainObject.type === UPLINK_CONSOLE_TYPE
    },

    view(_domainObject: unknown) {
      let unmount: (() => void) | null = null

      return {
        show(element: HTMLElement) {
          unmount = mountReactInShadow(
            element,
            <UplinkConsole openmct={openmct} />
          )
        },
        destroy() {
          unmount?.()
          unmount = null
        },
        priority() {
          return openmct.priority.HIGH
        },
      }
    },
  })

  openmct.objects.addRoot(
    { namespace: UPLINK_CONSOLE_NAMESPACE, key: UPLINK_CONSOLE_ROOT_KEY },
    openmct.priority.HIGH
  )

  openmct.objects.addProvider(UPLINK_CONSOLE_NAMESPACE, {
    get(identifier: { namespace: string; key: string }) {
      if (identifier.key === UPLINK_CONSOLE_ROOT_KEY) {
        return Promise.resolve({
          identifier,
          name: 'Send Uplink Console',
          type: UPLINK_CONSOLE_TYPE,
          location: 'ROOT',
        })
      }
      return Promise.resolve(undefined)
    },
  })
}

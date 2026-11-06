/**
 * Send Uplink control panel (§2 of `OPENMCT_MIGRATION_CONTEXT.md`).
 *
 * Reproduces the `rocket-cli` "Send Uplink" panel as an OpenMCT view: one
 * control per `VLPUplinkPacket` command, with the numeric input for target
 * apogee, the device picker for reset, and the multi-selectors for the AMP /
 * EPS power-output overwrites. Every command is confirmed before sending and
 * reports send-in-progress / success (with ACK RSSI/SNR) / failure-with-retry.
 *
 * NOTE: there is no ground-station uplink backend wired into NICK yet, so
 * {@link sendUplink} is a simulated stub — see its doc comment.
 */
import { useState, useCallback } from 'react'
import { NAMESPACE } from './data-provider'
import { mountReactInShadow } from './react-utils'
import { SectionLabel, ActionButton } from './ui'
import type { OpenMCT } from 'openmct'

export const UPLINK_PANEL_TYPE = `${NAMESPACE}.uplink-panel`

// ---------------------------------------------------------------------------
// Command model (§2)
// ---------------------------------------------------------------------------

/** `ChangeMode` targets. */
const MODES = ['LowPower', 'SelfTest', 'Armed', 'Landed', 'Demo'] as const
type Mode = (typeof MODES)[number]

/** `DeviceToReset` enum. */
const RESET_DEVICES = [
  'All',
  'VoidLake',
  'AMP',
  'AMPOut1',
  'AMPOut2',
  'AMPOut3',
  'AMPOut4',
  'Icarus',
  'PayloadActivationPCB',
  'RocketWifi',
  'OzysAll',
  'MainBulkhead',
  'DrogueBulkhead',
  'PayloadEPS1',
  'PayloadEPS2',
  'AeroRust',
] as const
type ResetDevice = (typeof RESET_DEVICES)[number]

/** `PowerOutputOverwrite` enum: option label → wire value. */
const OVERWRITE_OPTIONS = [
  { label: 'No Overwrite', value: 0 },
  { label: 'Enable', value: 1 },
  { label: 'Disable', value: 2 },
] as const

const AMP_OUTPUTS = ['Out 1', 'Out 2', 'Out 3', 'Out 4']
const EPS_OUTPUTS = [
  'EPS1 3.3V',
  'EPS1 5V',
  'EPS1 9V',
  'EPS2 3.3V',
  'EPS2 5V',
  'EPS2 9V',
]

/** A command sent to the rocket (`VLPUplinkPacket`). */
type UplinkCommand =
  | { type: 'SetTargetApogee'; meters: number }
  | { type: 'ChangeMode'; mode: Mode }
  | { type: 'Reset'; device: ResetDevice }
  | { type: 'AMPOutputOverwrite'; outputs: number[] }
  | { type: 'PayloadEPSOutputOverwrite'; outputs: number[] }
  | { type: 'FirePyro'; pyro: 'PyroMain' | 'PyroDrogue' }

/** Uplink acknowledgement returned by the ground station (§3.6). */
type Ack = { rssi: number; snr: number }

/**
 * Send an uplink command to the ground station and resolve with its ACK.
 *
 * SIMULATED: NICK has no ground-station uplink endpoint yet, so this fakes the
 * round-trip (latency, occasional failure, ACK RSSI/SNR). Replace the body with
 * a real POST to the GS backend when one exists; the UI contract (resolve with
 * {@link Ack}, reject with an Error) should stay the same.
 *
 * @param command the command to transmit
 * @returns the ground-station acknowledgement
 */
function sendTestUplink(command: UplinkCommand): Promise<Ack> {
  // TODO: make this have a real uplink, similar to uplink console
  // Stands in for the POST to the ground-station uplink endpoint.
  console.debug('[uplink] sending', command)
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      // ~20% simulated link failure so the retry path is exercisable.
      if (Math.random() < 0.2) {
        reject(new Error('No ACK received (link timeout)'))
        return
      }
      const rssi = -90 - Math.round(Math.random() * 25)
      const snr = 4 + Math.round(Math.random() * 8)
      resolve({ rssi, snr })
    }, 700)
  })
}

/** Human-readable summary of a command, used in confirms and feedback. */
function describe(command: UplinkCommand): string {
  switch (command.type) {
    case 'SetTargetApogee':
      return `Set Target Apogee → ${command.meters} m`
    case 'ChangeMode':
      return `Change Mode → ${command.mode}`
    case 'Reset':
      return `Reset Device → ${command.device}`
    case 'AMPOutputOverwrite':
      return `Overwrite AMP → [${command.outputs.join(', ')}]`
    case 'PayloadEPSOutputOverwrite':
      return `Overwrite EPS → [${command.outputs.join(', ')}]`
    case 'FirePyro':
      return `FIRE ${command.pyro}`
  }
}

// ---------------------------------------------------------------------------
// View state
// ---------------------------------------------------------------------------

type Feedback =
  | { status: 'idle' }
  | { status: 'sending'; label: string }
  | { status: 'success'; label: string; ack: Ack }
  | { status: 'error'; label: string; message: string }

/** A labeled <select> bound to the overwrite options. */
function OverwriteSelect({
  label,
  value,
  onChange,
}: {
  label: string
  value: number
  onChange: (value: number) => void
}) {
  return (
    <label className="flex items-center gap-1.5 text-[11px] text-gray-500 dark:text-gray-400">
      <span className="w-[72px] shrink-0">{label}</span>
      <select
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="flex-1 min-w-0 rounded border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-800 text-gray-800 dark:text-gray-100 text-[11px] px-1.5 py-1 outline-none focus:border-blue-500"
      >
        {OVERWRITE_OPTIONS.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </label>
  )
}

function UplinkPanel() {
  const [feedback, setFeedback] = useState<Feedback>({ status: 'idle' })
  const [lastCommand, setLastCommand] = useState<UplinkCommand | null>(null)

  const [apogee, setApogee] = useState('3000')
  const [resetDevice, setResetDevice] = useState<ResetDevice>('All')
  const [ampOverwrites, setAmpOverwrites] = useState<number[]>([0, 0, 0, 0])
  const [epsOverwrites, setEpsOverwrites] = useState<number[]>([
    0, 0, 0, 0, 0, 0,
  ])

  const isSending = feedback.status === 'sending'

  const dispatch = useCallback((command: UplinkCommand, strong = false) => {
    const summary = describe(command)
    const prompt = strong
      ? `⚠ DESTRUCTIVE COMMAND ⚠\n\n${summary}\n\nThis cannot be undone. Send anyway?`
      : `Send uplink command?\n\n${summary}`
    if (!confirm(prompt)) return

    setLastCommand(command)
    setFeedback({ status: 'sending', label: summary })
    sendTestUplink(command)
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
  }, [lastCommand, dispatch])

  const setAmpAt = (index: number, value: number) =>
    setAmpOverwrites((prev) => prev.map((v, i) => (i === index ? value : v)))
  const setEpsAt = (index: number, value: number) =>
    setEpsOverwrites((prev) => prev.map((v, i) => (i === index ? value : v)))

  return (
    <div className="h-full overflow-y-auto text-gray-800 dark:text-gray-100 text-[12px] font-sans p-3.5">
      <div className="flex flex-col gap-3.5">
        {/* Flight mode */}
        <div>
          <SectionLabel>Change Mode</SectionLabel>
          <div className="flex flex-wrap items-center gap-1.5">
            {MODES.map((mode) => (
              <ActionButton
                key={mode}
                disabled={isSending}
                onClick={() => dispatch({ type: 'ChangeMode', mode })}
              >
                {mode}
              </ActionButton>
            ))}
          </div>
        </div>

        <div className="border-t border-gray-200 dark:border-gray-700" />

        {/* Target apogee */}
        <div>
          <SectionLabel>Target Apogee</SectionLabel>
          <div className="flex items-center gap-2">
            <input
              type="number"
              min={0}
              max={10000}
              step={0.01}
              value={apogee}
              onChange={(e) => setApogee(e.target.value)}
              className="w-[110px] rounded border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-800 text-gray-800 dark:text-gray-100 text-[11px] px-2 py-1 outline-none focus:border-blue-500"
            />
            <span className="text-[11px] text-gray-400 dark:text-gray-500">
              m
            </span>
            <ActionButton
              variant="primary"
              disabled={isSending}
              onClick={() => {
                const meters = Number(apogee)
                if (!Number.isFinite(meters) || meters < 0 || meters > 10000) {
                  alert('Target apogee must be between 0 and 10000 m.')
                  return
                }
                dispatch({ type: 'SetTargetApogee', meters })
              }}
            >
              Send
            </ActionButton>
          </div>
        </div>

        <div className="border-t border-gray-200 dark:border-gray-700" />

        {/* Reset device */}
        <div>
          <SectionLabel>Reset Device</SectionLabel>
          <div className="flex items-center gap-2">
            <select
              value={resetDevice}
              onChange={(e) => setResetDevice(e.target.value as ResetDevice)}
              className="flex-1 min-w-0 rounded border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-800 text-gray-800 dark:text-gray-100 text-[11px] px-1.5 py-1 outline-none focus:border-blue-500"
            >
              {RESET_DEVICES.map((device) => (
                <option key={device} value={device}>
                  {device}
                </option>
              ))}
            </select>
            <ActionButton
              disabled={isSending}
              onClick={() => dispatch({ type: 'Reset', device: resetDevice })}
            >
              Reset
            </ActionButton>
          </div>
        </div>

        <div className="border-t border-gray-200 dark:border-gray-700" />

        {/* AMP output overwrite */}
        <div>
          <SectionLabel>Overwrite AMP Outputs</SectionLabel>
          <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 mb-2">
            {AMP_OUTPUTS.map((label, i) => (
              <OverwriteSelect
                key={label}
                label={label}
                value={ampOverwrites[i]}
                onChange={(v) => setAmpAt(i, v)}
              />
            ))}
          </div>
          <ActionButton
            disabled={isSending}
            onClick={() =>
              dispatch({ type: 'AMPOutputOverwrite', outputs: ampOverwrites })
            }
          >
            Send AMP Overwrite
          </ActionButton>
        </div>

        <div className="border-t border-gray-200 dark:border-gray-700" />

        {/* EPS output overwrite */}
        <div>
          <SectionLabel>Overwrite EPS Outputs</SectionLabel>
          <div className="grid grid-cols-2 gap-x-3 gap-y-1.5 mb-2">
            {EPS_OUTPUTS.map((label, i) => (
              <OverwriteSelect
                key={label}
                label={label}
                value={epsOverwrites[i]}
                onChange={(v) => setEpsAt(i, v)}
              />
            ))}
          </div>
          <ActionButton
            disabled={isSending}
            onClick={() =>
              dispatch({
                type: 'PayloadEPSOutputOverwrite',
                outputs: epsOverwrites,
              })
            }
          >
            Send EPS Overwrite
          </ActionButton>
        </div>

        <div className="border-t border-gray-200 dark:border-gray-700" />

        {/* Pyro (destructive) */}
        <div>
          <SectionLabel>Pyro (Destructive)</SectionLabel>
          <div className="flex flex-wrap items-center gap-1.5">
            <ActionButton
              variant="danger"
              disabled={isSending}
              onClick={() =>
                dispatch({ type: 'FirePyro', pyro: 'PyroMain' }, true)
              }
            >
              Fire Main Pyro
            </ActionButton>
            <ActionButton
              variant="danger"
              disabled={isSending}
              onClick={() =>
                dispatch({ type: 'FirePyro', pyro: 'PyroDrogue' }, true)
              }
            >
              Fire Drogue Pyro
            </ActionButton>
          </div>
        </div>

        {/* Feedback */}
        {feedback.status !== 'idle' && (
          <div className="border-t border-gray-200 dark:border-gray-700 pt-2.5">
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
    </div>
  )
}

export function UplinkPanelPlugin(openmct: OpenMCT) {
  openmct.types.addType(UPLINK_PANEL_TYPE, {
    name: 'Send Uplink',
    description: 'Send commands to the rocket and view ACK feedback',
    cssClass: 'icon-arrow-up',
  })

  openmct.objectViews.addProvider({
    key: `${UPLINK_PANEL_TYPE}.view`,
    name: 'Send Uplink',
    cssClass: 'icon-arrow-up',

    canView(domainObject: { type: string }) {
      return domainObject.type === UPLINK_PANEL_TYPE
    },

    view(_domainObject: unknown) {
      let unmount: (() => void) | null = null

      return {
        show(element: HTMLElement) {
          unmount = mountReactInShadow(element, <UplinkPanel />)
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
}

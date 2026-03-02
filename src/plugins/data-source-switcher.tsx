import { useState, useCallback } from 'react'
import {
  BACKEND_STORAGE_KEY,
  getBackendType,
  type BackendType,
} from '../db/get-backend'
import {
  loadInfluxConfig,
  saveInfluxConfig,
  testInfluxConfig,
  InfluxDBBackend,
} from '../db/influxdb'
import { getAllData, clearAllData } from '../db/duckdb'
import { NAMESPACE } from './data-provider'
import { mountReactInShadow } from './react-utils'

const LAST_UPLOAD_KEY = 'nick-last-upload-ts'
const UPLOAD_BATCH_SIZE = 5000

function formatTimestamp(ts: number | null): string {
  if (!ts) return 'Never'
  return new Date(ts).toLocaleString()
}

type View = 'main' | 'config'

type UploadState =
  | { status: 'idle' }
  | { status: 'running'; message: string }
  | { status: 'success'; message: string }
  | { status: 'error'; message: string }

type DeleteState =
  | { status: 'idle' }
  | { status: 'running' }
  | { status: 'error'; message: string }

type SaveState =
  | { status: 'idle' }
  | { status: 'running' }
  | { status: 'success' }
  | { status: 'error'; message: string }

type SwitchState =
  | { status: 'idle' }
  | { status: 'running'; target: BackendType }

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <p className="text-[11px] font-semibold uppercase tracking-[0.06em] text-gray-400 dark:text-gray-500 mb-1.5">
      {children}
    </p>
  )
}

function ActionButton({
  children,
  onClick,
  disabled,
  variant = 'default',
}: {
  children: React.ReactNode
  onClick?: () => void
  disabled?: boolean
  variant?: 'default' | 'primary'
}) {
  const base =
    'rounded px-3 py-1 text-[11px] cursor-pointer border transition-colors disabled:opacity-50 disabled:cursor-not-allowed'
  const styles =
    variant === 'primary'
      ? 'bg-blue-600 text-white border-blue-600 hover:bg-blue-700 hover:border-blue-700'
      : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 border-gray-300 dark:border-gray-600 hover:border-blue-500 hover:text-blue-600 dark:hover:text-blue-400'

  return (
    <button
      className={`${base} ${styles}`}
      onClick={onClick}
      disabled={disabled}
    >
      {children}
    </button>
  )
}

function BackendButton({
  label,
  type,
  active,
  switchState,
  onSwitch,
}: {
  label: string
  type: BackendType
  active: BackendType
  switchState: SwitchState
  onSwitch: (type: BackendType) => void
}) {
  const isActive = type === active
  const isLoading =
    switchState.status === 'running' && switchState.target === type

  return (
    <button
      className={[
        'rounded px-3 py-1 text-[11px] border transition-colors cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed',
        isActive
          ? 'bg-blue-600 text-white border-blue-600 font-semibold'
          : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-200 border-gray-300 dark:border-gray-600 hover:border-blue-500 hover:text-blue-600 dark:hover:text-blue-400',
      ].join(' ')}
      disabled={isLoading || switchState.status === 'running'}
      onClick={() => onSwitch(type)}
    >
      {isLoading ? 'Connecting…' : label}
    </button>
  )
}

function ConfigField({
  label,
  value,
  onChange,
  placeholder,
  isPassword,
}: {
  label: string
  value: string
  onChange: (v: string) => void
  placeholder: string
  isPassword?: boolean
}) {
  return (
    <div className="flex items-center gap-2 mb-1.5">
      <label className="w-[90px] shrink-0 text-[11px] text-gray-500 dark:text-gray-400">
        {label}
      </label>
      <input
        type={isPassword ? 'password' : 'text'}
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
        className="flex-1 min-w-0 rounded border border-gray-300 dark:border-gray-600 bg-gray-50 dark:bg-gray-800 text-gray-800 dark:text-gray-100 text-[11px] px-2 py-1 outline-none focus:border-blue-500 transition-colors placeholder:text-gray-400 dark:placeholder:text-gray-500"
      />
    </div>
  )
}

function DataSourceSwitcher() {
  const [view, setView] = useState<View>('main')
  const [currentBackend, setCurrentBackend] =
    useState<BackendType>(getBackendType())
  const [switchState, setSwitchState] = useState<SwitchState>({
    status: 'idle',
  })

  const [lastUploadTs, setLastUploadTs] = useState<number | null>(() => {
    const raw = localStorage.getItem(LAST_UPLOAD_KEY)
    return raw ? Number(raw) : null
  })
  const [uploadState, setUploadState] = useState<UploadState>({
    status: 'idle',
  })
  const [deleteState, setDeleteState] = useState<DeleteState>({
    status: 'idle',
  })

  const initialConfig = loadInfluxConfig()
  const [url, setUrl] = useState(initialConfig.url)
  const [token, setToken] = useState(initialConfig.token)
  const [org, setOrg] = useState(initialConfig.org)
  const [bucket, setBucket] = useState(initialConfig.bucket)
  const [saveState, setSaveState] = useState<SaveState>({ status: 'idle' })

  const handleSwitch = useCallback(
    async (type: BackendType) => {
      if (currentBackend === type) return
      if (type === 'influxdb') {
        setSwitchState({ status: 'running', target: type })
        const cfg = loadInfluxConfig()
        try {
          await testInfluxConfig(cfg.url, cfg.token, cfg.org)
        } catch (err) {
          setSwitchState({ status: 'idle' })
          const reason = err instanceof Error ? err.message : String(err)
          alert(
            `Cannot reach InfluxDB: ${reason}\n\nPlease open the ⚙ settings and verify your InfluxDB configuration.`
          )
          return
        }
      }
      localStorage.setItem(BACKEND_STORAGE_KEY, type)
      setCurrentBackend(type)
      location.reload()
    },
    [currentBackend]
  )

  const handleUpload = useCallback(async () => {
    setUploadState({ status: 'running', message: 'Reading local data…' })
    try {
      const afterTs = lastUploadTs ?? undefined
      const dataByKey = await getAllData(afterTs)

      const totalRows = Array.from(dataByKey.values()).reduce(
        (sum, rows) => sum + rows.length,
        0
      )
      if (totalRows === 0) {
        setUploadState({ status: 'success', message: 'No new data to upload.' })
        return
      }

      setUploadState({
        status: 'running',
        message: `Uploading ${totalRows} rows…`,
      })
      const influx = new InfluxDBBackend()

      let uploaded = 0
      let maxTs = 0
      for (const [key, rows] of dataByKey) {
        for (let i = 0; i < rows.length; i += UPLOAD_BATCH_SIZE) {
          const batch = rows.slice(i, i + UPLOAD_BATCH_SIZE)
          await influx.writeBatch(key, batch)
          uploaded += batch.length
          setUploadState({
            status: 'running',
            message: `Uploaded ${uploaded}/${totalRows} rows…`,
          })
        }
        if (rows.length > 0) {
          maxTs = Math.max(maxTs, rows[rows.length - 1].timestampMs)
        }
      }

      localStorage.setItem(LAST_UPLOAD_KEY, String(maxTs))
      setLastUploadTs(maxTs)
      setUploadState({
        status: 'success',
        message: `Done. ${totalRows} rows uploaded.`,
      })
    } catch (err) {
      setUploadState({
        status: 'error',
        message: err instanceof Error ? err.message : String(err),
      })
    }
  }, [lastUploadTs])

  const handleDelete = useCallback(async () => {
    if (!confirm('Delete all local DuckDB data? This cannot be undone.')) return
    setDeleteState({ status: 'running' })
    try {
      await clearAllData()
      localStorage.removeItem(LAST_UPLOAD_KEY)
      location.reload()
    } catch (err) {
      setDeleteState({
        status: 'error',
        message: err instanceof Error ? err.message : String(err),
      })
    }
  }, [])

  const handleSave = useCallback(async () => {
    setSaveState({ status: 'running' })
    try {
      await testInfluxConfig(url, token, org)
      saveInfluxConfig({ url, token, org, bucket })
      setSaveState({ status: 'success' })
      setTimeout(() => setSaveState({ status: 'idle' }), 3000)
    } catch (err) {
      setSaveState({
        status: 'error',
        message: err instanceof Error ? err.message : String(err),
      })
    }
  }, [url, token, org, bucket])

  return (
    <div className="h-full overflow-y-auto text-gray-800 dark:text-gray-100 text-[12px] font-sans p-3.5">
      {view === 'main' ? (
        <div className="flex flex-col gap-3.5">
          {/* Active Backend */}
          <div>
            <SectionLabel>Active Backend</SectionLabel>
            <div className="flex items-center gap-1.5">
              <BackendButton
                label="DuckDB (Local)"
                type="duckdb"
                active={currentBackend}
                switchState={switchState}
                onSwitch={handleSwitch}
              />
              <BackendButton
                label="InfluxDB (Remote)"
                type="influxdb"
                active={currentBackend}
                switchState={switchState}
                onSwitch={handleSwitch}
              />
              <button
                title="InfluxDB Configuration"
                onClick={() => setView('config')}
                className="text-[15px] text-gray-400 hover:text-blue-500 dark:hover:text-blue-400 transition-colors px-1 leading-none cursor-pointer bg-transparent border-none"
              >
                ⚙
              </button>
            </div>
          </div>

          <div className="border-t border-gray-200 dark:border-gray-700" />

          {/* Upload */}
          <div>
            <SectionLabel>Upload Local Data to InfluxDB</SectionLabel>
            <p className="text-[11px] text-gray-400 dark:text-gray-500 mb-2">
              Last uploaded: {formatTimestamp(lastUploadTs)}
            </p>
            <div className="flex items-center gap-2.5 flex-wrap">
              <ActionButton
                onClick={handleDelete}
                disabled={deleteState.status === 'running'}
              >
                Delete Local Data
              </ActionButton>
              <ActionButton
                onClick={handleUpload}
                disabled={uploadState.status === 'running'}
              >
                Upload New Data
              </ActionButton>
              {uploadState.status !== 'idle' && (
                <span
                  className={[
                    'text-[11px]',
                    uploadState.status === 'error'
                      ? 'text-red-500'
                      : uploadState.status === 'success'
                        ? 'text-green-600 dark:text-green-400'
                        : 'text-gray-400 dark:text-gray-500',
                  ].join(' ')}
                >
                  {uploadState.status === 'error'
                    ? `Error: ${uploadState.message}`
                    : uploadState.message}
                </span>
              )}
              {deleteState.status === 'error' && (
                <span className="text-[11px] text-red-500">
                  Error: {deleteState.message}
                </span>
              )}
            </div>
          </div>
        </div>
      ) : (
        <div className="flex flex-col">
          <SectionLabel>InfluxDB Configuration</SectionLabel>
          <ConfigField
            label="URL"
            value={url}
            onChange={setUrl}
            placeholder="http://localhost:8086"
          />
          <ConfigField
            label="API Token"
            value={token}
            onChange={setToken}
            placeholder="your-token-here"
            isPassword
          />
          <ConfigField
            label="Organization"
            value={org}
            onChange={setOrg}
            placeholder="rocketry"
          />
          <ConfigField
            label="Bucket"
            value={bucket}
            onChange={setBucket}
            placeholder="rocketry"
          />
          <div className="flex items-center gap-2 mt-2">
            <ActionButton onClick={() => setView('main')}>Close</ActionButton>
            <ActionButton
              variant="primary"
              onClick={handleSave}
              disabled={saveState.status === 'running'}
            >
              {saveState.status === 'running' ? 'Testing…' : 'Save & Test'}
            </ActionButton>
            {saveState.status === 'success' && (
              <span className="text-[11px] text-green-600 dark:text-green-400">
                Connected &amp; saved.
              </span>
            )}
            {saveState.status === 'error' && (
              <span className="text-[11px] text-red-500">
                Connection failed: {saveState.message}
              </span>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function DataSourceSwitcherPlugin(openmct: any) {
  openmct.types.addType(`${NAMESPACE}.data-source-switcher`, {
    name: 'Data Source Switcher',
    description: 'Configure and switch between DuckDB and InfluxDB backends',
    cssClass: 'icon-database',
  })

  openmct.objectViews.addProvider({
    key: `${NAMESPACE}.data-source-switcher.view`,
    name: 'Data Source Switcher',
    cssClass: 'icon-database',

    canView(domainObject: { type: string }) {
      return domainObject.type === `${NAMESPACE}.data-source-switcher`
    },

    view(_domainObject: unknown) {
      let unmount: (() => void) | null = null

      return {
        show(element: HTMLElement) {
          unmount = mountReactInShadow(element, <DataSourceSwitcher />)
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

import {
  BACKEND_STORAGE_KEY,
  getBackendType,
  type BackendType,
} from './backend'
import {
  loadInfluxConfig,
  saveInfluxConfig,
  InfluxDBBackend,
  INFLUXDB_URL_KEY,
  INFLUXDB_TOKEN_KEY,
  INFLUXDB_ORG_KEY,
  INFLUXDB_BUCKET_KEY,
} from './influxdb'
import { getAllData } from './duckdb'

const LAST_UPLOAD_KEY = 'caduceus-last-upload-ts'
const UPLOAD_BATCH_SIZE = 5000

function formatTimestamp(ts: number | null): string {
  if (!ts) return 'Never'
  return new Date(ts).toLocaleString()
}

function buildUI(container: HTMLElement): void {
  container.innerHTML = ''
  container.style.cssText = `
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 16px;
    height: 100%;
    overflow-y: auto;
    box-sizing: border-box;
    color: var(--colorBodyFg, #fff);
    font-family: var(--fontFace, 'Helvetica Neue', Helvetica, Arial, sans-serif);
    font-size: 12px;
  `

  // ── Section: Backend Switch ──────────────────────────────────────────────
  const switchSection = document.createElement('div')
  switchSection.style.cssText = sectionStyle()

  const switchTitle = document.createElement('div')
  switchTitle.textContent = 'Active Backend'
  switchTitle.style.cssText = sectionTitleStyle()
  switchSection.appendChild(switchTitle)

  const switchRow = document.createElement('div')
  switchRow.style.cssText = 'display: flex; gap: 8px; align-items: center;'

  const currentBackend = getBackendType()

  const duckdbBtn = makeBackendButton('DuckDB (Local)', 'duckdb', currentBackend)
  const influxBtn = makeBackendButton('InfluxDB (Remote)', 'influxdb', currentBackend)

  function makeBackendButton(label: string, type: BackendType, active: BackendType) {
    const btn = document.createElement('button')
    btn.textContent = label
    btn.style.cssText = buttonStyle(type === active)
    btn.addEventListener('click', () => {
      if (getBackendType() === type) return
      localStorage.setItem(BACKEND_STORAGE_KEY, type)
      location.reload()
    })
    return btn
  }

  switchRow.appendChild(duckdbBtn)
  switchRow.appendChild(influxBtn)
  switchSection.appendChild(switchRow)

  const activeLabel = document.createElement('div')
  activeLabel.textContent = `Currently using: ${currentBackend === 'duckdb' ? 'DuckDB (Local)' : 'InfluxDB (Remote)'}`
  activeLabel.style.cssText = `
    margin-top: 6px;
    color: var(--colorStatusDefault, #aaa);
    font-size: 11px;
  `
  switchSection.appendChild(activeLabel)
  container.appendChild(switchSection)

  // ── Section: InfluxDB Config ─────────────────────────────────────────────
  const configSection = document.createElement('div')
  configSection.style.cssText = sectionStyle()

  const configTitle = document.createElement('div')
  configTitle.textContent = 'InfluxDB Configuration'
  configTitle.style.cssText = sectionTitleStyle()
  configSection.appendChild(configTitle)

  const config = loadInfluxConfig()

  const urlField = makeField('URL', INFLUXDB_URL_KEY, config.url, 'http://localhost:8086')
  const tokenField = makeField('API Token', INFLUXDB_TOKEN_KEY, config.token, 'your-token-here', true)
  const orgField = makeField('Organization', INFLUXDB_ORG_KEY, config.org, 'my-org')
  const bucketField = makeField('Bucket', INFLUXDB_BUCKET_KEY, config.bucket, 'telemetry')

  configSection.appendChild(urlField.row)
  configSection.appendChild(tokenField.row)
  configSection.appendChild(orgField.row)
  configSection.appendChild(bucketField.row)

  const saveRow = document.createElement('div')
  saveRow.style.cssText = 'display: flex; align-items: center; gap: 10px; margin-top: 4px;'

  const saveBtn = document.createElement('button')
  saveBtn.textContent = 'Save Config'
  saveBtn.style.cssText = buttonStyle(false)

  const saveStatus = document.createElement('span')
  saveStatus.style.cssText = 'font-size: 11px; color: var(--colorStatusDefault, #aaa);'

  saveBtn.addEventListener('click', () => {
    saveInfluxConfig({
      url: urlField.input.value.trim(),
      token: tokenField.input.value.trim(),
      org: orgField.input.value.trim(),
      bucket: bucketField.input.value.trim(),
    })
    saveStatus.textContent = 'Saved.'
    setTimeout(() => {
      saveStatus.textContent = ''
    }, 2000)
  })

  saveRow.appendChild(saveBtn)
  saveRow.appendChild(saveStatus)
  configSection.appendChild(saveRow)
  container.appendChild(configSection)

  // ── Section: Upload DuckDB → InfluxDB ────────────────────────────────────
  const uploadSection = document.createElement('div')
  uploadSection.style.cssText = sectionStyle()

  const uploadTitle = document.createElement('div')
  uploadTitle.textContent = 'Upload Local Data to InfluxDB'
  uploadTitle.style.cssText = sectionTitleStyle()
  uploadSection.appendChild(uploadTitle)

  const lastUploadTs = localStorage.getItem(LAST_UPLOAD_KEY)
  const lastUploadLabel = document.createElement('div')
  lastUploadLabel.style.cssText = `
    margin-bottom: 8px;
    color: var(--colorStatusDefault, #aaa);
    font-size: 11px;
  `
  lastUploadLabel.textContent = `Last uploaded: ${formatTimestamp(lastUploadTs ? Number(lastUploadTs) : null)}`
  uploadSection.appendChild(lastUploadLabel)

  const uploadRow = document.createElement('div')
  uploadRow.style.cssText = 'display: flex; align-items: center; gap: 10px;'

  const uploadBtn = document.createElement('button')
  uploadBtn.textContent = 'Upload New Data'
  uploadBtn.style.cssText = buttonStyle(false)

  const uploadStatus = document.createElement('span')
  uploadStatus.style.cssText = 'font-size: 11px; color: var(--colorStatusDefault, #aaa);'

  uploadBtn.addEventListener('click', async () => {
    uploadBtn.disabled = true
    uploadBtn.style.opacity = '0.6'
    uploadStatus.textContent = 'Reading local data…'

    try {
      const afterTs = lastUploadTs ? Number(lastUploadTs) : undefined
      const data = await getAllData(afterTs)

      if (data.length === 0) {
        uploadStatus.textContent = 'No new data to upload.'
        return
      }

      uploadStatus.textContent = `Uploading ${data.length} rows…`

      const currentConfig = loadInfluxConfig()
      const influx = new InfluxDBBackend()
      // Re-read config in case user just saved
      void currentConfig

      let uploaded = 0
      for (let i = 0; i < data.length; i += UPLOAD_BATCH_SIZE) {
        const batch = data.slice(i, i + UPLOAD_BATCH_SIZE)
        await influx.writeBatch(batch)
        uploaded += batch.length
        uploadStatus.textContent = `Uploaded ${uploaded}/${data.length} rows…`
      }

      // Record the highest received_timestamp we uploaded.
      // data is sorted ASC by received_timestamp, so the last element is the max.
      const maxTs = data[data.length - 1].receivedTimestamp
      localStorage.setItem(LAST_UPLOAD_KEY, String(maxTs))
      lastUploadLabel.textContent = `Last uploaded: ${formatTimestamp(maxTs)}`
      uploadStatus.textContent = `Done. ${data.length} rows uploaded.`
    } catch (err) {
      uploadStatus.textContent = `Error: ${err instanceof Error ? err.message : String(err)}`
    } finally {
      uploadBtn.disabled = false
      uploadBtn.style.opacity = '1'
    }
  })

  uploadRow.appendChild(uploadBtn)
  uploadRow.appendChild(uploadStatus)
  uploadSection.appendChild(uploadRow)
  container.appendChild(uploadSection)
}

function makeField(
  label: string,
  _storageKey: string,
  value: string,
  placeholder: string,
  isPassword = false,
) {
  const row = document.createElement('div')
  row.style.cssText = 'display: flex; align-items: center; gap: 8px; margin-bottom: 6px;'

  const lbl = document.createElement('label')
  lbl.textContent = label
  lbl.style.cssText = `
    width: 90px;
    flex-shrink: 0;
    color: var(--colorBodyFg, #fff);
    font-size: 11px;
  `

  const input = document.createElement('input')
  input.type = isPassword ? 'password' : 'text'
  input.value = value
  input.placeholder = placeholder
  input.style.cssText = `
    flex: 1;
    background: var(--colorInputBg, #1c1c1f);
    color: var(--colorInputFg, #fff);
    border: 1px solid var(--colorInputBorder, #555);
    border-radius: 3px;
    padding: 4px 8px;
    font-size: 11px;
    outline: none;
    min-width: 0;
  `

  row.appendChild(lbl)
  row.appendChild(input)
  return { row, input }
}

function sectionStyle(): string {
  return `
    background: var(--colorBodyBg, #1c1c1f);
    border: 1px solid var(--colorInteriorBorder, #333);
    border-radius: 4px;
    padding: 12px;
  `
}

function sectionTitleStyle(): string {
  return `
    font-size: 12px;
    font-weight: bold;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--colorBodyFg, #fff);
    margin-bottom: 10px;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--colorInteriorBorder, #333);
  `
}

function buttonStyle(active: boolean): string {
  return `
    background: ${active ? 'var(--colorBtnBgActive, #00b4ff)' : 'var(--colorBtnBg, #2e2e33)'};
    color: ${active ? 'var(--colorBtnFgActive, #000)' : 'var(--colorBtnFg, #fff)'};
    border: 1px solid ${active ? 'var(--colorBtnBorderActive, #00b4ff)' : 'var(--colorBtnBorder, #555)'};
    border-radius: 3px;
    padding: 5px 12px;
    font-size: 11px;
    cursor: pointer;
    font-family: inherit;
  `
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function DataSourceSwitcherPlugin(openmct: any) {
  const NAMESPACE = 'caduceus'
  const KEY = 'data-source-switcher'

  openmct.types.addType('caduceus.data-source-switcher', {
    name: 'Data Source Switcher',
    description: 'Configure and switch between DuckDB and InfluxDB backends',
    cssClass: 'icon-database',
  })

  openmct.objectViews.addProvider({
    key: 'caduceus.data-source-switcher.view',
    name: 'Data Source Switcher',
    cssClass: 'icon-database',

    canView(domainObject: { type: string }) {
      return domainObject.type === 'caduceus.data-source-switcher'
    },

    view(_domainObject: unknown) {
      let container: HTMLElement | null = null

      return {
        show(element: HTMLElement) {
          container = element
          buildUI(container)
        },
        destroy() {
          if (container) {
            container.innerHTML = ''
          }
        },
        priority() {
          return openmct.priority.HIGH
        },
      }
    },
  })

  // Ensure the object is resolvable (provider already added in telemetry-plugin,
  // but we add a fallback here in case this plugin is installed independently)
  openmct.objects.addProvider(`${NAMESPACE}-dss-fallback`, {
    get(identifier: { namespace: string; key: string }) {
      if (identifier.namespace === NAMESPACE && identifier.key === KEY) {
        return Promise.resolve({
          identifier,
          name: 'Data Source Switcher',
          type: 'caduceus.data-source-switcher',
          location: `${NAMESPACE}:layout`,
        })
      }
      return Promise.resolve(undefined)
    },
  })
}

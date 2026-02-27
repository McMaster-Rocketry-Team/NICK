import {
  BACKEND_STORAGE_KEY,
  getBackendType,
  type BackendType,
} from '../db/get-backend'
import {
  loadInfluxConfig,
  saveInfluxConfig,
  InfluxDBBackend,
  INFLUXDB_URL_KEY,
  INFLUXDB_TOKEN_KEY,
  INFLUXDB_ORG_KEY,
  INFLUXDB_BUCKET_KEY,
} from '../db/influxdb'
import { getAllData, clearAllData } from '../db/duckdb'
import { NAMESPACE } from './data-provider'

const LAST_UPLOAD_KEY = 'caduceus-last-upload-ts'
const UPLOAD_BATCH_SIZE = 5000

function formatTimestamp(ts: number | null): string {
  if (!ts) return 'Never'
  return new Date(ts).toLocaleString()
}

async function testInfluxConfig(
  url: string,
  token: string,
  org: string
): Promise<void> {
  const resp = await fetch(
    `${url}/api/v2/buckets?org=${encodeURIComponent(org)}&limit=1`,
    {
      headers: { Authorization: `Token ${token}` },
    }
  )
  if (!resp.ok) {
    const body = await resp.text().catch(() => '')
    let msg = `HTTP ${resp.status}`
    try {
      const json = JSON.parse(body)
      if (json.message) msg = json.message
    } catch {
      // ignore
    }
    throw new Error(msg)
  }
}

function buildUI(container: HTMLElement): void {
  container.innerHTML = ''
  container.style.cssText = `
    display: flex;
    flex-direction: column;
    padding: 14px 16px;
    height: 100%;
    overflow-y: auto;
    box-sizing: border-box;
    color: #333;
    font-family: var(--fontFace, 'Helvetica Neue', Helvetica, Arial, sans-serif);
    font-size: 12px;
    background: #fff;
  `

  // ── Main panel ───────────────────────────────────────────────────────────
  const mainPanel = document.createElement('div')
  mainPanel.style.cssText = 'display: flex; flex-direction: column; gap: 14px;'
  container.appendChild(mainPanel)

  // Active Backend section
  const currentBackend = getBackendType()

  const backendLabel = document.createElement('div')
  backendLabel.style.cssText = `
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #888;
    margin-bottom: 6px;
  `
  backendLabel.textContent = 'Active Backend'
  mainPanel.appendChild(backendLabel)

  const switchRow = document.createElement('div')
  switchRow.style.cssText = 'display: flex; gap: 6px; align-items: center;'

  const duckdbBtn = makeBackendButton(
    'DuckDB (Local)',
    'duckdb',
    currentBackend
  )
  const influxBtn = makeBackendButton(
    'InfluxDB (Remote)',
    'influxdb',
    currentBackend,
    async () => {
      const cfg = loadInfluxConfig()
      try {
        await testInfluxConfig(cfg.url, cfg.token, cfg.org)
      } catch (err) {
        const reason = err instanceof Error ? err.message : String(err)
        alert(
          `Cannot reach InfluxDB: ${reason}\n\nPlease open the ⚙ settings and verify your InfluxDB configuration.`
        )
        return false
      }
      return true
    }
  )

  // ── Settings gear button (inline, after InfluxDB button) ─────────────────
  const gearBtn = document.createElement('button')
  gearBtn.innerHTML = '&#9881;'
  gearBtn.title = 'InfluxDB Configuration'
  gearBtn.style.cssText = `
    background: none;
    border: none;
    font-size: 15px;
    cursor: pointer;
    color: #888;
    padding: 2px 4px;
    line-height: 1;
    border-radius: 3px;
    margin-left: 2px;
  `
  gearBtn.addEventListener('mouseenter', () => {
    gearBtn.style.color = '#0066cc'
  })
  gearBtn.addEventListener('mouseleave', () => {
    gearBtn.style.color = '#888'
  })

  switchRow.appendChild(duckdbBtn)
  switchRow.appendChild(influxBtn)
  switchRow.appendChild(gearBtn)
  mainPanel.appendChild(switchRow)

  // Upload section
  const divider = document.createElement('div')
  divider.style.cssText = 'border-top: 1px solid #e0e0e0; margin: 2px 0;'
  mainPanel.appendChild(divider)

  const uploadLabel = document.createElement('div')
  uploadLabel.style.cssText = `
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #888;
    margin-bottom: 6px;
  `
  uploadLabel.textContent = 'Upload Local Data to InfluxDB'
  mainPanel.appendChild(uploadLabel)

  const lastUploadTs = localStorage.getItem(LAST_UPLOAD_KEY)
  const lastUploadLabel = document.createElement('div')
  lastUploadLabel.style.cssText = 'font-size: 11px; color: #888;'
  lastUploadLabel.textContent = `Last uploaded: ${formatTimestamp(lastUploadTs ? Number(lastUploadTs) : null)}`
  mainPanel.appendChild(lastUploadLabel)

  const uploadRow = document.createElement('div')
  uploadRow.style.cssText = 'display: flex; align-items: center; gap: 10px;'

  const uploadBtn = document.createElement('button')
  uploadBtn.textContent = 'Upload New Data'
  uploadBtn.style.cssText = actionButtonStyle()

  const uploadStatus = document.createElement('span')
  uploadStatus.style.cssText = 'font-size: 11px; color: #888;'

  uploadBtn.addEventListener('click', async () => {
    uploadBtn.disabled = true
    uploadBtn.style.opacity = '0.6'
    uploadStatus.style.color = '#888'
    uploadStatus.textContent = 'Reading local data…'

    try {
      const afterTs = lastUploadTs ? Number(lastUploadTs) : undefined
      const dataByKey = await getAllData(afterTs)

      const totalRows = Array.from(dataByKey.values()).reduce(
        (sum, rows) => sum + rows.length,
        0
      )
      if (totalRows === 0) {
        uploadStatus.textContent = 'No new data to upload.'
        return
      }

      uploadStatus.textContent = `Uploading ${totalRows} rows…`
      const influx = new InfluxDBBackend()

      let uploaded = 0
      let maxTs = 0
      for (const [key, rows] of dataByKey) {
        for (let i = 0; i < rows.length; i += UPLOAD_BATCH_SIZE) {
          const batch = rows.slice(i, i + UPLOAD_BATCH_SIZE)
          await influx.writeBatch(key, batch)
          uploaded += batch.length
          uploadStatus.textContent = `Uploaded ${uploaded}/${totalRows} rows…`
        }
        if (rows.length > 0) {
          maxTs = Math.max(maxTs, rows[rows.length - 1].timestampMs)
        }
      }

      localStorage.setItem(LAST_UPLOAD_KEY, String(maxTs))
      lastUploadLabel.textContent = `Last uploaded: ${formatTimestamp(maxTs)}`
      uploadStatus.textContent = `Done. ${totalRows} rows uploaded.`
    } catch (err) {
      uploadStatus.style.color = '#cc0000'
      uploadStatus.textContent = `Error: ${err instanceof Error ? err.message : String(err)}`
    } finally {
      uploadBtn.disabled = false
      uploadBtn.style.opacity = '1'
    }
  })

  const deleteBtn = document.createElement('button')
  deleteBtn.textContent = 'Delete Local Data'
  deleteBtn.style.cssText = actionButtonStyle()

  const deleteStatus = document.createElement('span')
  deleteStatus.style.cssText = 'font-size: 11px; color: #888;'

  deleteBtn.addEventListener('click', async () => {
    if (!confirm('Delete all local DuckDB data? This cannot be undone.')) return
    deleteBtn.disabled = true
    deleteBtn.style.opacity = '0.6'
    deleteStatus.style.color = '#888'
    deleteStatus.textContent = 'Deleting…'
    try {
      await clearAllData()
      localStorage.removeItem(LAST_UPLOAD_KEY)
      location.reload()
    } catch (err) {
      deleteStatus.style.color = '#cc0000'
      deleteStatus.textContent = `Error: ${err instanceof Error ? err.message : String(err)}`
      deleteBtn.disabled = false
      deleteBtn.style.opacity = '1'
    }
  })

  uploadRow.appendChild(deleteBtn)
  uploadRow.appendChild(uploadBtn)
  uploadRow.appendChild(uploadStatus)
  uploadRow.appendChild(deleteStatus)
  mainPanel.appendChild(uploadRow)

  // ── Config panel (hidden by default) ────────────────────────────────────
  const configPanel = document.createElement('div')
  configPanel.style.cssText = 'display: none; flex-direction: column; gap: 0;'
  container.appendChild(configPanel)

  const configHeading = document.createElement('div')
  configHeading.style.cssText = `
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #888;
    margin-bottom: 10px;
  `
  configHeading.textContent = 'InfluxDB Configuration'
  configPanel.appendChild(configHeading)

  const config = loadInfluxConfig()
  const urlField = makeField(
    'URL',
    INFLUXDB_URL_KEY,
    config.url,
    'http://localhost:8086'
  )
  const tokenField = makeField(
    'API Token',
    INFLUXDB_TOKEN_KEY,
    config.token,
    'your-token-here',
    true
  )
  const orgField = makeField(
    'Organization',
    INFLUXDB_ORG_KEY,
    config.org,
    'rocketry'
  )
  const bucketField = makeField(
    'Bucket',
    INFLUXDB_BUCKET_KEY,
    config.bucket,
    'rocketry'
  )

  configPanel.appendChild(urlField.row)
  configPanel.appendChild(tokenField.row)
  configPanel.appendChild(orgField.row)
  configPanel.appendChild(bucketField.row)

  const saveRow = document.createElement('div')
  saveRow.style.cssText =
    'display: flex; align-items: center; gap: 8px; margin-top: 8px;'

  const closeBtn = document.createElement('button')
  closeBtn.textContent = 'Close'
  closeBtn.style.cssText = `
    background: none;
    border: 1px solid #ccc;
    border-radius: 3px;
    padding: 5px 12px;
    font-size: 11px;
    cursor: pointer;
    color: #555;
    font-family: inherit;
  `
  closeBtn.addEventListener('mouseenter', () => {
    closeBtn.style.borderColor = '#0066cc'
    closeBtn.style.color = '#0066cc'
  })
  closeBtn.addEventListener('mouseleave', () => {
    closeBtn.style.borderColor = '#ccc'
    closeBtn.style.color = '#555'
  })

  const saveBtn = document.createElement('button')
  saveBtn.textContent = 'Save & Test'
  saveBtn.style.cssText = actionButtonStyle()

  const saveStatus = document.createElement('span')
  saveStatus.style.cssText = 'font-size: 11px;'

  saveBtn.addEventListener('click', async () => {
    saveBtn.disabled = true
    saveBtn.style.opacity = '0.6'
    saveStatus.style.color = '#888'
    saveStatus.textContent = 'Testing connection…'

    const newConfig = {
      url: urlField.input.value.trim(),
      token: tokenField.input.value.trim(),
      org: orgField.input.value.trim(),
      bucket: bucketField.input.value.trim(),
    }

    try {
      await testInfluxConfig(newConfig.url, newConfig.token, newConfig.org)
      saveInfluxConfig(newConfig)
      saveStatus.style.color = '#2a7a2a'
      saveStatus.textContent = 'Connected & saved.'
      setTimeout(() => {
        saveStatus.textContent = ''
      }, 3000)
    } catch (err) {
      saveStatus.style.color = '#cc0000'
      saveStatus.textContent = `Connection failed: ${err instanceof Error ? err.message : String(err)}`
    } finally {
      saveBtn.disabled = false
      saveBtn.style.opacity = '1'
    }
  })

  saveRow.appendChild(closeBtn)
  saveRow.appendChild(saveBtn)
  saveRow.appendChild(saveStatus)
  configPanel.appendChild(saveRow)

  // ── Toggle logic ─────────────────────────────────────────────────────────
  function openConfig() {
    mainPanel.style.display = 'none'
    configPanel.style.display = 'flex'
  }

  function closeConfig() {
    mainPanel.style.display = 'flex'
    configPanel.style.display = 'none'
  }

  gearBtn.addEventListener('click', openConfig)
  closeBtn.addEventListener('click', closeConfig)
}

function makeBackendButton(
  label: string,
  type: BackendType,
  active: BackendType,
  guard?: () => Promise<boolean>
) {
  const btn = document.createElement('button')
  btn.textContent = label
  const isActive = type === active
  btn.style.cssText = `
    background: ${isActive ? '#0066cc' : '#f0f0f0'};
    color: ${isActive ? '#fff' : '#333'};
    border: 1px solid ${isActive ? '#0066cc' : '#ccc'};
    border-radius: 3px;
    padding: 5px 12px;
    font-size: 11px;
    cursor: pointer;
    font-family: inherit;
    font-weight: ${isActive ? '600' : '400'};
  `
  btn.addEventListener('click', async () => {
    if (getBackendType() === type) return
    if (guard) {
      btn.disabled = true
      btn.style.opacity = '0.6'
      const ok = await guard()
      btn.disabled = false
      btn.style.opacity = '1'
      if (!ok) return
    }
    localStorage.setItem(BACKEND_STORAGE_KEY, type)
    location.reload()
  })
  return btn
}

function makeField(
  label: string,
  _storageKey: string,
  value: string,
  placeholder: string,
  isPassword = false
) {
  const row = document.createElement('div')
  row.style.cssText =
    'display: flex; align-items: center; gap: 8px; margin-bottom: 7px;'

  const lbl = document.createElement('label')
  lbl.textContent = label
  lbl.style.cssText = `
    width: 90px;
    flex-shrink: 0;
    color: #555;
    font-size: 11px;
  `

  const input = document.createElement('input')
  input.type = isPassword ? 'password' : 'text'
  input.value = value
  input.placeholder = placeholder
  input.style.cssText = `
    flex: 1;
    background: #f8f8f8;
    color: #222;
    border: 1px solid #ccc;
    border-radius: 3px;
    padding: 4px 8px;
    font-size: 11px;
    outline: none;
    min-width: 0;
    font-family: inherit;
  `
  input.addEventListener('focus', () => {
    input.style.borderColor = '#0066cc'
  })
  input.addEventListener('blur', () => {
    input.style.borderColor = '#ccc'
  })

  row.appendChild(lbl)
  row.appendChild(input)
  return { row, input }
}

function actionButtonStyle(): string {
  return `
    background: #f0f0f0;
    color: #333;
    border: 1px solid #ccc;
    border-radius: 3px;
    padding: 5px 12px;
    font-size: 11px;
    cursor: pointer;
    font-family: inherit;
  `
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
}

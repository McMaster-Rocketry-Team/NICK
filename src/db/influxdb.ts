import type { TelemetryBackend, TelemetryDatum, QueryOptions } from './backend'

export const INFLUXDB_URL_KEY = 'influxdb-url'
export const INFLUXDB_TOKEN_KEY = 'influxdb-token'
export const INFLUXDB_ORG_KEY = 'influxdb-org'
export const INFLUXDB_BUCKET_KEY = 'influxdb-bucket'

export interface InfluxDBConfig {
  url: string
  token: string
  org: string
  bucket: string
}

export function loadInfluxConfig(): InfluxDBConfig {
  return {
    url: localStorage.getItem(INFLUXDB_URL_KEY) ?? 'http://localhost:8086',
    token: localStorage.getItem(INFLUXDB_TOKEN_KEY) ?? '',
    org: localStorage.getItem(INFLUXDB_ORG_KEY) ?? 'rocketry',
    bucket: localStorage.getItem(INFLUXDB_BUCKET_KEY) ?? 'rocketry',
  }
}

export function saveInfluxConfig(config: InfluxDBConfig): void {
  localStorage.setItem(INFLUXDB_URL_KEY, config.url)
  localStorage.setItem(INFLUXDB_TOKEN_KEY, config.token)
  localStorage.setItem(INFLUXDB_ORG_KEY, config.org)
  localStorage.setItem(INFLUXDB_BUCKET_KEY, config.bucket)
}

// Split a single CSV line respecting quoted fields.
function splitCSVLine(line: string): string[] {
  const cols: string[] = []
  let cur = ''
  let inQuote = false
  for (let i = 0; i < line.length; i++) {
    const ch = line[i]
    if (ch === '"') {
      inQuote = !inQuote
    } else if (ch === ',' && !inQuote) {
      cols.push(cur)
      cur = ''
    } else {
      cur += ch
    }
  }
  cols.push(cur)
  return cols
}

// Parse InfluxDB annotated CSV response into TelemetryDatum[].
// Schema: _time = timestampMs (ms precision), value = numeric value.
// InfluxDB annotated CSV format:
//   - Annotation rows start with '#' (skipped)
//   - Header row contains "_time" as one of its column values
//   - Blank lines separate logical tables (reset headers)
function parseFluxCSV(csv: string): TelemetryDatum[] {
  const lines = csv.split('\n')
  const results: TelemetryDatum[] = []
  let headers: string[] = []

  for (const line of lines) {
    const trimmed = line.trim()

    if (!trimmed) {
      headers = []
      continue
    }

    if (trimmed.startsWith('#')) continue

    const cols = splitCSVLine(trimmed)

    if (cols[0] === '' && cols.includes('_time')) {
      headers = cols
      continue
    }

    if (headers.length === 0) continue

    const row: Record<string, string> = {}
    for (let i = 0; i < headers.length; i++) {
      row[headers[i]] = cols[i] ?? ''
    }

    const timeStr = row['_time']
    const valueStr = row['value'] ?? row['_value']

    if (!timeStr || !valueStr) continue

    const timestampMs = new Date(timeStr).getTime()
    const value = parseFloat(valueStr)

    if (isNaN(timestampMs) || isNaN(value)) continue

    results.push({ timestampMs, value })
  }

  return results
}

export class InfluxDBBackend implements TelemetryBackend {
  private config: InfluxDBConfig

  constructor(config?: InfluxDBConfig) {
    this.config = config ?? loadInfluxConfig()
  }

  async init(): Promise<void> {
    // No async initialization needed for HTTP backend
  }

  private authHeaders(): HeadersInit {
    return {
      Authorization: `Token ${this.config.token}`,
      'Content-Type': 'text/plain; charset=utf-8',
    }
  }

  async insertTelemetry(
    key: string,
    timestampMs: number,
    value: number
  ): Promise<void> {
    const { url, org, bucket } = this.config
    const line = `${key} value=${value} ${timestampMs}`

    const writeUrl = `${url}/api/v2/write?org=${encodeURIComponent(org)}&bucket=${encodeURIComponent(bucket)}&precision=ms`

    const resp = await fetch(writeUrl, {
      method: 'POST',
      headers: this.authHeaders(),
      body: line,
    })

    if (!resp.ok) {
      const body = await resp.text()
      throw new Error(`InfluxDB write failed: ${resp.status} ${body}`)
    }
  }

  async writeBatch(key: string, data: TelemetryDatum[]): Promise<void> {
    if (data.length === 0) return

    const { url, org, bucket } = this.config
    const lines: string[] = []

    for (const datum of data) {
      lines.push(`${key} value=${datum.value} ${datum.timestampMs}`)
    }

    const writeUrl = `${url}/api/v2/write?org=${encodeURIComponent(org)}&bucket=${encodeURIComponent(bucket)}&precision=ms`

    const resp = await fetch(writeUrl, {
      method: 'POST',
      headers: this.authHeaders(),
      body: lines.join('\n'),
    })

    if (!resp.ok) {
      const body = await resp.text()
      throw new Error(`InfluxDB batch write failed: ${resp.status} ${body}`)
    }
  }

  async queryTelemetry(
    key: string,
    start: number,
    end: number,
    options?: QueryOptions
  ): Promise<TelemetryDatum[]> {
    const { url, org, bucket } = this.config
    const strategy = options?.strategy
    const size = options?.size

    const startRfc = new Date(start).toISOString()
    const endRfc = new Date(end).toISOString()

    const baseQuery = `
from(bucket: "${bucket}")
  |> range(start: ${startRfc}, stop: ${endRfc})
  |> filter(fn: (r) => r._measurement == "${key}")
  |> filter(fn: (r) => r._field == "value")`

    let flux: string

    if (strategy === 'latest' && size) {
      flux = `${baseQuery}
  |> sort(columns: ["_time"], desc: false)
  |> tail(n: ${size})`
    } else if (strategy === 'minmax' && size && size > 0) {
      const totalMs = end - start
      const windowMs = Math.max(1, Math.floor(totalMs / size))
      const windowDur = `${windowMs}ms`

      const makeSelQuery = (fn: string) => `
from(bucket: "${bucket}")
  |> range(start: ${startRfc}, stop: ${endRfc})
  |> filter(fn: (r) => r._measurement == "${key}")
  |> filter(fn: (r) => r._field == "value")
  |> group()
  |> window(every: ${windowDur})
  |> ${fn}(column: "_value")
  |> group()`

      const queryUrl = `${url}/api/v2/query?org=${encodeURIComponent(org)}`
      const headers = {
        Authorization: `Token ${this.config.token}`,
        'Content-Type': 'application/vnd.flux' as const,
        Accept: 'application/csv' as const,
      }

      const [minResp, maxResp] = await Promise.all([
        fetch(queryUrl, { method: 'POST', headers, body: makeSelQuery('min') }),
        fetch(queryUrl, { method: 'POST', headers, body: makeSelQuery('max') }),
      ])

      if (!minResp.ok) {
        const body = await minResp.text()
        throw new Error(`InfluxDB min query failed: ${minResp.status} ${body}`)
      }
      if (!maxResp.ok) {
        const body = await maxResp.text()
        throw new Error(`InfluxDB max query failed: ${maxResp.status} ${body}`)
      }

      const [minCsv, maxCsv] = await Promise.all([
        minResp.text(),
        maxResp.text(),
      ])
      const minPoints = parseFluxCSV(minCsv)
      const maxPoints = parseFluxCSV(maxCsv)

      const maxByTime = new Map(maxPoints.map((p) => [p.timestampMs, p]))

      const merged: TelemetryDatum[] = []
      for (const minP of minPoints) {
        merged.push(minP)
        const maxP = maxByTime.get(minP.timestampMs)
        if (maxP && maxP.value !== minP.value) {
          merged.push(maxP)
        }
      }
      const minTimes = new Set(minPoints.map((p) => p.timestampMs))
      for (const maxP of maxPoints) {
        if (!minTimes.has(maxP.timestampMs)) {
          merged.push(maxP)
        }
      }

      merged.sort((a, b) => a.timestampMs - b.timestampMs)
      return merged
    } else {
      flux = baseQuery
    }

    const queryUrl = `${url}/api/v2/query?org=${encodeURIComponent(org)}`
    const resp = await fetch(queryUrl, {
      method: 'POST',
      headers: {
        Authorization: `Token ${this.config.token}`,
        'Content-Type': 'application/vnd.flux',
        Accept: 'application/csv',
      },
      body: flux,
    })

    if (!resp.ok) {
      const body = await resp.text()
      throw new Error(`InfluxDB query failed: ${resp.status} ${body}`)
    }

    const csv = await resp.text()
    return parseFluxCSV(csv)
  }
}

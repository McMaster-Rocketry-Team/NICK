import type { TelemetryBackend, TelemetryDatum, TelemetrySeries, QueryOptions } from './backend'

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
    org: localStorage.getItem(INFLUXDB_ORG_KEY) ?? '',
    bucket: localStorage.getItem(INFLUXDB_BUCKET_KEY) ?? '',
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
// InfluxDB annotated CSV format (confirmed from real responses):
//   - Annotation rows start with '#' (skipped)
//   - Both header and data rows have an empty string as cols[0]
//   - Header row is distinguished by containing "_time" as one of its column values
//   - Data rows have an ISO timestamp in the _time column position
//   - Blank lines separate logical tables (reset headers)
// After pivot, field columns are named "value" and "alt_ts" (not "_value").
function parseFluxCSV(csv: string, series: TelemetrySeries): TelemetryDatum[] {
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

    // Header row: first col is empty AND the row contains "_time" as a column name
    if (cols[0] === '' && cols.includes('_time')) {
      headers = cols
      continue
    }

    if (headers.length === 0) continue

    // Data row
    const row: Record<string, string> = {}
    for (let i = 0; i < headers.length; i++) {
      row[headers[i]] = cols[i] ?? ''
    }

    const timeStr = row['_time']
    // After pivot, the field column is named "value"; fall back to "_value" for non-pivoted queries
    const valueStr = row['value'] ?? row['_value']
    const altTsStr = row['alt_ts']

    if (!timeStr || !valueStr) continue

    const domainTs = new Date(timeStr).getTime()
    const altTs = altTsStr ? Number(altTsStr) : domainTs
    const value = parseFloat(valueStr)

    if (isNaN(domainTs) || isNaN(value)) continue

    if (series === 'gps') {
      // domainTs = GPS timestamp, altTs = received timestamp
      results.push({ timestamp: domainTs, receivedTimestamp: altTs, value })
    } else {
      // domainTs = received timestamp, altTs = GPS timestamp (or 0 if no GPS fix)
      const hasGps = altTsStr && altTsStr !== '0' && altTsStr !== ''
      results.push({
        timestamp: hasGps ? altTs : null,
        receivedTimestamp: domainTs,
        value,
      })
    }
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
    _table: string,
    timestamp: number | null,
    receivedTimestamp: number,
    value: number,
  ): Promise<void> {
    const { url, org, bucket } = this.config

    // For GPS series: _time = GPS timestamp, alt_ts = received timestamp
    // For received series: _time = received timestamp, alt_ts = 0 (no GPS)
    const isGps = timestamp !== null
    const domainTs = isGps ? timestamp! : receivedTimestamp
    const altTs = isGps ? receivedTimestamp : 0
    const seriesTag = isGps ? 'gps' : 'received'

    const line = `vl_battery_v,series=${seriesTag} value=${value},alt_ts=${altTs}i ${domainTs}`

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

  async writeBatch(data: TelemetryDatum[]): Promise<void> {
    if (data.length === 0) return

    const { url, org, bucket } = this.config
    const lines: string[] = []

    for (const datum of data) {
      const isGps = datum.timestamp !== null
      const domainTs = isGps ? datum.timestamp! : datum.receivedTimestamp
      const altTs = isGps ? datum.receivedTimestamp : 0
      const seriesTag = isGps ? 'gps' : 'received'
      lines.push(
        `vl_battery_v,series=${seriesTag} value=${datum.value},alt_ts=${altTs}i ${domainTs}`,
      )
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
    _table: string,
    series: TelemetrySeries,
    start: number,
    end: number,
    options?: QueryOptions,
  ): Promise<TelemetryDatum[]> {
    const { url, org, bucket } = this.config
    const strategy = options?.strategy
    const size = options?.size

    const startRfc = new Date(start).toISOString()
    const endRfc = new Date(end).toISOString()
    const seriesTag = series === 'gps' ? 'gps' : 'received'

    let flux: string

    const baseQuery = `
from(bucket: "${bucket}")
  |> range(start: ${startRfc}, stop: ${endRfc})
  |> filter(fn: (r) => r._measurement == "vl_battery_v" and r.series == "${seriesTag}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "value", "alt_ts"])`

    if (strategy === 'latest' && size) {
      flux = `${baseQuery}
  |> sort(columns: ["_time"], desc: false)
  |> tail(n: ${size})`
    } else if (strategy === 'minmax' && size && size > 0) {
      const totalMs = end - start
      const windowMs = Math.max(1, Math.floor(totalMs / size))
      const windowDur = `${windowMs}ms`

      // Use mean aggregation per window — produces one smooth value per pixel-width bucket.
      // A true min/max union via Flux produces separate tables that can't be cleanly
      // interleaved, causing spikes. Mean gives faithful visual representation at this zoom.
      flux = `
from(bucket: "${bucket}")
  |> range(start: ${startRfc}, stop: ${endRfc})
  |> filter(fn: (r) => r._measurement == "vl_battery_v" and r.series == "${seriesTag}" and r._field == "value")
  |> aggregateWindow(every: ${windowDur}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value"])
  |> rename(columns: {_value: "value"})`

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
      return this.parseAggregatedCSV(csv, series)
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
    return parseFluxCSV(csv, series)
  }

  // Parse aggregated (no alt_ts) CSV — used for minmax/mean strategy results.
  private parseAggregatedCSV(csv: string, series: TelemetrySeries): TelemetryDatum[] {
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

      const domainTs = new Date(timeStr).getTime()
      const value = parseFloat(valueStr)
      if (isNaN(domainTs) || isNaN(value)) continue

      if (series === 'gps') {
        results.push({ timestamp: domainTs, receivedTimestamp: domainTs, value })
      } else {
        results.push({ timestamp: null, receivedTimestamp: domainTs, value })
      }
    }

    return results
  }
}

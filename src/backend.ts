export interface TelemetryDatum {
  timestamp: number | null
  receivedTimestamp: number
  value: number
}

export type TelemetrySeries = 'gps' | 'received'

export interface QueryOptions {
  strategy?: 'minmax' | 'latest'
  size?: number
}

export interface TelemetryBackend {
  init(): Promise<void>
  insertTelemetry(
    table: string,
    timestamp: number | null,
    receivedTimestamp: number,
    value: number,
  ): Promise<void>
  queryTelemetry(
    table: string,
    series: TelemetrySeries,
    start: number,
    end: number,
    options?: QueryOptions,
  ): Promise<TelemetryDatum[]>
}

export type BackendType = 'duckdb' | 'influxdb'

export const BACKEND_STORAGE_KEY = 'caduceus-backend'

export function getBackendType(): BackendType {
  const stored = localStorage.getItem(BACKEND_STORAGE_KEY)
  if (stored === 'influxdb') return 'influxdb'
  return 'duckdb'
}

let backendInstance: TelemetryBackend | null = null

export async function getBackend(): Promise<TelemetryBackend> {
  if (backendInstance) return backendInstance

  const type = getBackendType()

  if (type === 'influxdb') {
    const { InfluxDBBackend } = await import('./influxdb')
    backendInstance = new InfluxDBBackend()
  } else {
    const { DuckDBBackend } = await import('./duckdb')
    backendInstance = new DuckDBBackend()
  }

  await backendInstance.init()
  return backendInstance
}

import type { TelemetryBackend } from './backend'
import { DuckDBBackend } from './duckdb'
import { InfluxDBBackend } from './influxdb'

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
  const instance =
    type === 'influxdb' ? new InfluxDBBackend() : new DuckDBBackend()

  await instance.init()
  backendInstance = instance
  return instance
}

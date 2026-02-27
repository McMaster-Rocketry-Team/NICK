import { getConnection, DuckDBBackend } from '../db/duckdb'

const TABLE_NAME = 'vl_battery_v'
const TICK_INTERVAL_MS = 10

export type Datum = { timestamp: number | null; receivedTimestamp: number; value: number }
export type Callback = (datum: Datum) => void

const subscribers = new Set<Callback>()
let generatorInterval: ReturnType<typeof setInterval> | null = null
let hasGpsFix = true

// Singleton DuckDB backend for local capture (only used when DuckDB is the active backend)
const localDuckDB = new DuckDBBackend()

function startGenerator() {
  if (generatorInterval !== null) return

  // Initialize DuckDB for local data capture
  getConnection().catch(console.error)

  // Toggle GPS fix on/off every 2 seconds
  setInterval(() => {
    hasGpsFix = !hasGpsFix
  }, 2000)

  generatorInterval = setInterval(() => {
    const receivedTimestamp = Date.now()
    const timestamp = hasGpsFix ? receivedTimestamp : null
    const value = Math.sin((2 * Math.PI * receivedTimestamp) / 10000)
    const datum: Datum = { timestamp, receivedTimestamp, value }

    // Always write to DuckDB for local capture
    localDuckDB.insertTelemetry(TABLE_NAME, timestamp, receivedTimestamp, value).catch(console.error)

    for (const cb of subscribers) {
      cb(datum)
    }
  }, TICK_INTERVAL_MS)
}

export function subscribe(callback: Callback): () => void {
  subscribers.add(callback)
  startGenerator()
  return () => {
    subscribers.delete(callback)
  }
}

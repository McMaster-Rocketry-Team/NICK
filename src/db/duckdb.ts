import * as duckdb from '@duckdb/duckdb-wasm'
import type { Table } from 'apache-arrow'
import type { TelemetryBackend, TelemetryDatum, QueryOptions } from './backend'
import {
  ensureTable,
  insertTelemetrySQL,
  getAllDataSQL,
  queryTelemetrySQL,
  type Row,
} from './duckdb-store'

let dbInstance: duckdb.AsyncDuckDB | null = null
let connInstance: duckdb.AsyncDuckDBConnection | null = null
let initPromise: Promise<duckdb.AsyncDuckDBConnection> | null = null

// Convert an Arrow Table result into the Row[] shape expected by duckdb-store helpers.
function arrowToRows(result: Table): Row[] {
  const rows: Row[] = []
  for (const batch of result.batches) {
    const timestamps = batch.getChildAt(0)
    const values = batch.getChildAt(1)
    if (!timestamps || !values) continue
    for (let i = 0; i < batch.numRows; i++) {
      rows.push({
        timestamp: BigInt(timestamps.get(i)),
        value: Number(values.get(i)),
      })
    }
  }
  return rows
}

async function initDuckDB(): Promise<duckdb.AsyncDuckDBConnection> {
  const bundles = duckdb.getJsDelivrBundles()
  const bundle = await duckdb.selectBundle(bundles)

  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker!}");`], {
      type: 'text/javascript',
    })
  )

  const worker = new Worker(workerUrl)
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR)
  const db = new duckdb.AsyncDuckDB(logger, worker)
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker)
  URL.revokeObjectURL(workerUrl)

  await db.open({
    path: 'opfs://nick.db',
    accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
  })

  const conn = await db.connect()

  dbInstance = db
  connInstance = conn

  const CHECKPOINT_INTERVAL_MS = 1000
  setInterval(() => {
    conn.query('CHECKPOINT').catch(console.error)
  }, CHECKPOINT_INTERVAL_MS)

  return conn
}

export async function getConnection(): Promise<duckdb.AsyncDuckDBConnection> {
  if (connInstance) return connInstance
  if (!initPromise) {
    initPromise = initDuckDB()
  }
  return initPromise
}

/**
 * Terminates the DuckDB worker (releasing the OPFS file lock) then deletes
 * the database file, wiping all stored telemetry.
 * The caller must reload the page afterwards.
 */
export async function clearAllData(): Promise<void> {
  if (dbInstance) {
    await dbInstance.terminate()
    dbInstance = null
    connInstance = null
    initPromise = null
  }
  const root = await navigator.storage.getDirectory()
  await root.removeEntry('nick.db', { recursive: true }).catch(() => {
    // File may not exist if no data was ever written; ignore
  })
}

/**
 * Reads all telemetry from every table in the database.
 * @param afterTimestamp  if provided, only returns rows with timestamp > this value
 * @returns map of key → datums for every table that exists
 */
export async function getAllData(
  afterTimestamp?: number
): Promise<Map<string, TelemetryDatum[]>> {
  const conn = await getConnection()
  const queryFn = (sql: string) => conn.query(sql).then(arrowToRows)
  const result = await conn.query(
    `SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'`
  )
  const tables: string[] = []
  for (const batch of result.batches) {
    const col = batch.getChildAt(0)
    if (!col) continue
    for (let i = 0; i < batch.numRows; i++) {
      tables.push(String(col.get(i)))
    }
  }
  return getAllDataSQL(queryFn, tables, afterTimestamp)
}

export class DuckDBBackend implements TelemetryBackend {
  async init(): Promise<void> {
    await getConnection()
  }

  async insertTelemetry(
    key: string,
    timestampMs: number,
    value: number
  ): Promise<void> {
    const conn = await getConnection()
    const queryFn = (sql: string) => conn.query(sql).then(arrowToRows)
    await ensureTable(queryFn, key)
    await insertTelemetrySQL(queryFn, key, timestampMs, value)
  }

  async queryTelemetry(
    key: string,
    start: number,
    end: number,
    options?: QueryOptions
  ): Promise<TelemetryDatum[]> {
    const conn = await getConnection()
    const queryFn = (sql: string) => conn.query(sql).then(arrowToRows)
    return queryTelemetrySQL(queryFn, key, start, end, options)
  }
}

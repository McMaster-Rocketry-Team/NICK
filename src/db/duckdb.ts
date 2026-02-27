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
    path: 'opfs://caduceus.db',
    accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
  })

  const conn = await db.connect()

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

export async function getAllData(
  afterTimestamp?: number
): Promise<TelemetryDatum[]> {
  const conn = await getConnection()
  const queryFn = (sql: string) => conn.query(sql).then(arrowToRows)
  return getAllDataSQL(queryFn, afterTimestamp)
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

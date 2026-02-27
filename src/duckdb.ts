import * as duckdb from '@duckdb/duckdb-wasm'
import type { Table } from 'apache-arrow'
import type { TelemetryBackend, TelemetryDatum, TelemetrySeries, QueryOptions } from './backend'
import {
  setupSchema,
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
    const receivedTimestamps = batch.getChildAt(1)
    const values = batch.getChildAt(2)
    if (!timestamps || !receivedTimestamps || !values) continue
    for (let i = 0; i < batch.numRows; i++) {
      const ts = timestamps.get(i)
      rows.push({
        timestamp: ts === null ? null : BigInt(ts),
        received_timestamp: BigInt(receivedTimestamps.get(i)),
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
    }),
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
  const queryFn = (sql: string) => conn.query(sql).then(arrowToRows)
  await setupSchema(queryFn)

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

export async function getAllData(afterReceivedTimestamp?: number): Promise<TelemetryDatum[]> {
  const conn = await getConnection()
  const queryFn = (sql: string) => conn.query(sql).then(arrowToRows)
  return getAllDataSQL(queryFn, afterReceivedTimestamp)
}

export class DuckDBBackend implements TelemetryBackend {
  async init(): Promise<void> {
    await getConnection()
  }

  async insertTelemetry(
    table: string,
    timestamp: number | null,
    receivedTimestamp: number,
    value: number,
  ): Promise<void> {
    const conn = await getConnection()
    const queryFn = (sql: string) => conn.query(sql).then(arrowToRows)
    await insertTelemetrySQL(queryFn, table, timestamp, receivedTimestamp, value)
  }

  async queryTelemetry(
    table: string,
    series: TelemetrySeries,
    start: number,
    end: number,
    options?: QueryOptions,
  ): Promise<TelemetryDatum[]> {
    const conn = await getConnection()
    const queryFn = (sql: string) => conn.query(sql).then(arrowToRows)
    return queryTelemetrySQL(queryFn, table, series, start, end, options)
  }
}

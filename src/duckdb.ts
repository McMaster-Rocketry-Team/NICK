import * as duckdb from '@duckdb/duckdb-wasm'
import type { Table } from 'apache-arrow'

export interface TelemetryDatum {
  timestamp: number
  value: number
}

let connInstance: duckdb.AsyncDuckDBConnection | null = null
let initPromise: Promise<duckdb.AsyncDuckDBConnection> | null = null

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
  await conn.query(`
    CREATE TABLE IF NOT EXISTS vl_battery_v (
      timestamp BIGINT NOT NULL,
      value DOUBLE NOT NULL
    )
  `)

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

export async function insertTelemetry(
  table: string,
  timestamp: number,
  value: number,
): Promise<void> {
  const conn = await getConnection()
  await conn.query(
    `INSERT INTO ${table} VALUES (${timestamp}, ${value})`,
  )
}

function readBatches(result: Table): TelemetryDatum[] {
  const raw: TelemetryDatum[] = []
  for (const batch of result.batches) {
    const timestamps = batch.getChildAt(0)
    const values = batch.getChildAt(1)
    if (!timestamps || !values) continue
    for (let i = 0; i < batch.numRows; i++) {
      raw.push({
        timestamp: Number(timestamps.get(i)),
        value: Number(values.get(i)),
      })
    }
  }
  return raw
}

export interface QueryOptions {
  strategy?: 'minmax' | 'latest'
  size?: number
}

export async function queryTelemetry(
  table: string,
  start: number,
  end: number,
  options?: QueryOptions,
): Promise<TelemetryDatum[]> {
  const conn = await getConnection()
  const strategy = options?.strategy
  const size = options?.size

  if (strategy === 'latest' && size) {
    const result = await conn.query(
      `SELECT timestamp, value FROM ${table}
       WHERE timestamp >= ${start} AND timestamp <= ${end}
       ORDER BY timestamp DESC
       LIMIT ${size}`,
    )
    const raw = readBatches(result)
    raw.reverse()
    return raw
  }

  if (strategy === 'minmax' && size && size > 0) {
    const buckets = Math.max(1, Math.floor(size / 2))
    const result = await conn.query(
      `WITH bucketed AS (
        SELECT timestamp, value,
          NTILE(${buckets}) OVER (ORDER BY timestamp) AS bucket
        FROM ${table}
        WHERE timestamp >= ${start} AND timestamp <= ${end}
      ),
      extremes AS (
        SELECT
          arg_min(timestamp, value) AS min_ts, MIN(value) AS min_val,
          arg_max(timestamp, value) AS max_ts, MAX(value) AS max_val
        FROM bucketed
        GROUP BY bucket
      )
      SELECT timestamp, value FROM (
        SELECT min_ts AS timestamp, min_val AS value FROM extremes
        UNION ALL
        SELECT max_ts AS timestamp, max_val AS value FROM extremes
        WHERE max_ts != min_ts
      )
      ORDER BY timestamp ASC`,
    )
    return readBatches(result)
  }

  // No strategy / no size: return all points
  const result = await conn.query(
    `SELECT timestamp, value FROM ${table}
     WHERE timestamp >= ${start} AND timestamp <= ${end}
     ORDER BY timestamp ASC`,
  )
  return readBatches(result)
}

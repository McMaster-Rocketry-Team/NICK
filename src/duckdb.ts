import * as duckdb from '@duckdb/duckdb-wasm'
import type { Table } from 'apache-arrow'
import type { TelemetryBackend, TelemetryDatum, TelemetrySeries, QueryOptions } from './backend'

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

  // Migrate: drop and recreate if schema is outdated (missing received_timestamp column)
  const cols = await conn.query(
    `SELECT column_name FROM information_schema.columns WHERE table_name = 'vl_battery_v'`,
  )
  const colNames = cols.toArray().map((r: { column_name: string }) => r.column_name)
  if (colNames.length > 0 && !colNames.includes('received_timestamp')) {
    await conn.query(`DROP TABLE vl_battery_v`)
  }

  await conn.query(`
    CREATE TABLE IF NOT EXISTS vl_battery_v (
      timestamp BIGINT,
      received_timestamp BIGINT NOT NULL,
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

function readBatches(result: Table): TelemetryDatum[] {
  const raw: TelemetryDatum[] = []
  for (const batch of result.batches) {
    const timestamps = batch.getChildAt(0)
    const receivedTimestamps = batch.getChildAt(1)
    const values = batch.getChildAt(2)
    if (!timestamps || !receivedTimestamps || !values) continue
    for (let i = 0; i < batch.numRows; i++) {
      const ts = timestamps.get(i)
      raw.push({
        timestamp: ts === null ? null : Number(ts),
        receivedTimestamp: Number(receivedTimestamps.get(i)),
        value: Number(values.get(i)),
      })
    }
  }
  return raw
}

export async function getAllData(afterReceivedTimestamp?: number): Promise<TelemetryDatum[]> {
  const conn = await getConnection()
  const filter =
    afterReceivedTimestamp !== undefined
      ? `WHERE received_timestamp > ${afterReceivedTimestamp}`
      : ''
  const result = await conn.query(
    `SELECT timestamp, received_timestamp, value FROM vl_battery_v ${filter} ORDER BY received_timestamp ASC`,
  )
  return readBatches(result)
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
    const tsValue = timestamp === null ? 'NULL' : timestamp
    await conn.query(
      `INSERT INTO ${table} VALUES (${tsValue}, ${receivedTimestamp}, ${value})`,
    )
  }

  async queryTelemetry(
    table: string,
    series: TelemetrySeries,
    start: number,
    end: number,
    options?: QueryOptions,
  ): Promise<TelemetryDatum[]> {
    const conn = await getConnection()
    const strategy = options?.strategy
    const size = options?.size

    const domainCol = series === 'gps' ? 'timestamp' : 'received_timestamp'
    const seriesFilter =
      series === 'gps' ? 'AND timestamp IS NOT NULL' : 'AND timestamp IS NULL'

    if (strategy === 'latest' && size) {
      const result = await conn.query(
        `SELECT timestamp, received_timestamp, value FROM ${table}
         WHERE ${domainCol} >= ${start} AND ${domainCol} <= ${end} ${seriesFilter}
         ORDER BY ${domainCol} DESC
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
          SELECT timestamp, received_timestamp, value,
            NTILE(${buckets}) OVER (ORDER BY ${domainCol}) AS bucket
          FROM ${table}
          WHERE ${domainCol} >= ${start} AND ${domainCol} <= ${end} ${seriesFilter}
        ),
        extremes AS (
          SELECT
            arg_min(timestamp, value) AS min_ts,
            arg_min(received_timestamp, value) AS min_rts,
            MIN(value) AS min_val,
            arg_max(timestamp, value) AS max_ts,
            arg_max(received_timestamp, value) AS max_rts,
            MAX(value) AS max_val
          FROM bucketed
          GROUP BY bucket
        )
        SELECT timestamp, received_timestamp, value FROM (
          SELECT min_ts AS timestamp, min_rts AS received_timestamp, min_val AS value FROM extremes
          UNION ALL
          SELECT max_ts AS timestamp, max_rts AS received_timestamp, max_val AS value FROM extremes
          WHERE max_ts != min_ts OR max_rts != min_rts
        )
        ORDER BY ${domainCol} ASC`,
      )
      return readBatches(result)
    }

    const result = await conn.query(
      `SELECT timestamp, received_timestamp, value FROM ${table}
       WHERE ${domainCol} >= ${start} AND ${domainCol} <= ${end} ${seriesFilter}
       ORDER BY ${domainCol} ASC`,
    )
    return readBatches(result)
  }
}

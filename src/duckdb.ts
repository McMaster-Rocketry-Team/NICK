import * as duckdb from '@duckdb/duckdb-wasm'

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

// If two consecutive points are further apart than this, insert a NaN gap
// marker so the plot line breaks instead of drawing a long diagonal.
const GAP_THRESHOLD_MS = 200

export async function queryTelemetry(
  table: string,
  start: number,
  end: number,
): Promise<TelemetryDatum[]> {
  const conn = await getConnection()
  const result = await conn.query(
    `SELECT timestamp, value FROM ${table}
     WHERE timestamp >= ${start} AND timestamp <= ${end}
     ORDER BY timestamp ASC`,
  )

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

  const datums: TelemetryDatum[] = []

  const firstTs = raw.length > 0 ? raw[0].timestamp : undefined
  if (firstTs !== undefined && firstTs - start > GAP_THRESHOLD_MS) {
    datums.push({ timestamp: start, value: NaN })
  }

  for (let i = 0; i < raw.length; i++) {
    if (i > 0 && raw[i].timestamp - raw[i - 1].timestamp > GAP_THRESHOLD_MS) {
      datums.push({ timestamp: raw[i - 1].timestamp + 1, value: NaN })
    }
    datums.push(raw[i])
  }

  const lastTs = raw.length > 0 ? raw[raw.length - 1].timestamp : undefined
  if (lastTs !== undefined && end - lastTs > GAP_THRESHOLD_MS) {
    datums.push({ timestamp: lastTs + 1, value: NaN })
  }

  return datums
}

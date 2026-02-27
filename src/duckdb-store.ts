// Pure SQL logic for the DuckDB telemetry store.
// Operates on a generic query function so it can be used with both the
// browser WASM backend and the Node native backend in tests.

import type { TelemetryDatum, TelemetrySeries, QueryOptions } from './backend'

export type Row = { timestamp: bigint | null; received_timestamp: bigint; value: number }

export type QueryFn = (sql: string) => Promise<Row[]>

export async function setupSchema(query: QueryFn): Promise<void> {
  // Migrate: drop if schema is outdated (missing received_timestamp column)
  const cols = (await query(
    `SELECT column_name FROM information_schema.columns WHERE table_name = 'vl_battery_v'`,
  )) as unknown as { column_name: string }[]
  const colNames = cols.map((r) => r.column_name)
  if (colNames.length > 0 && !colNames.includes('received_timestamp')) {
    await query(`DROP TABLE vl_battery_v`)
  }

  await query(`
    CREATE TABLE IF NOT EXISTS vl_battery_v (
      timestamp BIGINT,
      received_timestamp BIGINT NOT NULL,
      value DOUBLE NOT NULL
    )
  `)
}

function toTelemetryDatum(row: Row): TelemetryDatum {
  const ts = row.timestamp
  return {
    timestamp: ts === null ? null : Number(ts),
    receivedTimestamp: Number(row.received_timestamp),
    value: Number(row.value),
  }
}

export async function insertTelemetrySQL(
  query: QueryFn,
  table: string,
  timestamp: number | null,
  receivedTimestamp: number,
  value: number,
): Promise<void> {
  const tsValue = timestamp === null ? 'NULL' : timestamp
  await query(`INSERT INTO ${table} VALUES (${tsValue}, ${receivedTimestamp}, ${value})`)
}

export async function getAllDataSQL(
  query: QueryFn,
  afterReceivedTimestamp?: number,
): Promise<TelemetryDatum[]> {
  const filter =
    afterReceivedTimestamp !== undefined
      ? `WHERE received_timestamp > ${afterReceivedTimestamp}`
      : ''
  const rows = await query(
    `SELECT timestamp, received_timestamp, value FROM vl_battery_v ${filter} ORDER BY received_timestamp ASC`,
  )
  return rows.map(toTelemetryDatum)
}

export async function queryTelemetrySQL(
  query: QueryFn,
  table: string,
  series: TelemetrySeries,
  start: number,
  end: number,
  options?: QueryOptions,
): Promise<TelemetryDatum[]> {
  const strategy = options?.strategy
  const size = options?.size

  const domainCol = series === 'gps' ? 'timestamp' : 'received_timestamp'
  const seriesFilter = series === 'gps' ? 'AND timestamp IS NOT NULL' : 'AND timestamp IS NULL'

  if (strategy === 'latest' && size) {
    const rows = await query(
      `SELECT timestamp, received_timestamp, value FROM ${table}
       WHERE ${domainCol} >= ${start} AND ${domainCol} <= ${end} ${seriesFilter}
       ORDER BY ${domainCol} DESC
       LIMIT ${size}`,
    )
    const result = rows.map(toTelemetryDatum)
    result.reverse()
    return result
  }

  if (strategy === 'minmax' && size && size > 0) {
    const buckets = Math.max(1, Math.floor(size / 2))
    const rows = await query(
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
    return rows.map(toTelemetryDatum)
  }

  const rows = await query(
    `SELECT timestamp, received_timestamp, value FROM ${table}
     WHERE ${domainCol} >= ${start} AND ${domainCol} <= ${end} ${seriesFilter}
     ORDER BY ${domainCol} ASC`,
  )
  return rows.map(toTelemetryDatum)
}

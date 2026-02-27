// Pure SQL logic for the DuckDB telemetry store.
// Operates on a generic query function so it can be used with both the
// browser WASM backend and the Node native backend in tests.

import type { TelemetryDatum, QueryOptions } from './backend'

export type Row = {
  timestamp: bigint
  value: number
}

export type QueryFn = (sql: string) => Promise<Row[]>

/**
 * Creates the table for `key` if it does not already exist.
 * Called lazily before every insert so new keys are handled automatically.
 */
export async function ensureTable(
  query: QueryFn,
  tableName: string
): Promise<void> {
  await query(`
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      timestamp BIGINT NOT NULL,
      value DOUBLE NOT NULL
    )
  `)
}

function toTelemetryDatum(row: Row): TelemetryDatum {
  return {
    timestampMs: Number(row.timestamp),
    value: Number(row.value),
  }
}

export async function insertTelemetrySQL(
  query: QueryFn,
  table: string,
  timestampMs: number,
  value: number
): Promise<void> {
  await query(`INSERT INTO "${table}" VALUES (${timestampMs}, ${value})`)
}

/**
 * Reads all telemetry from the given tables, returning a map of key → datums.
 * @param tables  list of table names (keys) to read from
 * @param afterTimestamp  if provided, only returns rows with timestamp > this value
 */
export async function getAllDataSQL(
  query: QueryFn,
  tables: string[],
  afterTimestamp?: number
): Promise<Map<string, TelemetryDatum[]>> {
  const filter =
    afterTimestamp !== undefined ? `WHERE timestamp > ${afterTimestamp}` : ''
  const result = new Map<string, TelemetryDatum[]>()
  for (const table of tables) {
    const rows = await query(
      `SELECT timestamp, value FROM "${table}" ${filter} ORDER BY timestamp ASC`
    )
    result.set(table, rows.map(toTelemetryDatum))
  }
  return result
}

export async function queryTelemetrySQL(
  query: QueryFn,
  table: string,
  start: number,
  end: number,
  options?: QueryOptions
): Promise<TelemetryDatum[]> {
  await ensureTable(query, table)
  const strategy = options?.strategy
  const size = options?.size

  if (strategy === 'latest' && size) {
    const rows = await query(
      `SELECT timestamp, value FROM "${table}"
       WHERE timestamp >= ${start} AND timestamp <= ${end}
       ORDER BY timestamp DESC
       LIMIT ${size}`
    )
    const result = rows.map(toTelemetryDatum)
    result.reverse()
    return result
  }

  if (strategy === 'minmax' && size && size > 0) {
    const buckets = Math.max(1, size)
    const rows = await query(
      `WITH bucketed AS (
        SELECT timestamp, value,
          NTILE(${buckets}) OVER (ORDER BY timestamp) AS bucket
        FROM "${table}"
        WHERE timestamp >= ${start} AND timestamp <= ${end}
      ),
      extremes AS (
        SELECT
          arg_min(timestamp, value) AS min_ts,
          MIN(value) AS min_val,
          arg_max(timestamp, value) AS max_ts,
          MAX(value) AS max_val
        FROM bucketed
        GROUP BY bucket
      )
      SELECT timestamp, value FROM (
        SELECT min_ts AS timestamp, min_val AS value FROM extremes
        UNION ALL
        SELECT max_ts AS timestamp, max_val AS value FROM extremes
        WHERE max_ts != min_ts
      )
      ORDER BY timestamp ASC`
    )
    return rows.map(toTelemetryDatum)
  }

  const rows = await query(
    `SELECT timestamp, value FROM "${table}"
     WHERE timestamp >= ${start} AND timestamp <= ${end}
     ORDER BY timestamp ASC`
  )
  return rows.map(toTelemetryDatum)
}

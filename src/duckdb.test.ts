import { describe, it, expect } from 'vitest'
import { DuckDBInstance } from '@duckdb/node-api'
import {
  setupSchema,
  insertTelemetrySQL,
  getAllDataSQL,
  queryTelemetrySQL,
  type Row,
} from './duckdb-store'

const TABLE = 'vl_battery_v'

// Build a QueryFn from a node-api connection that returns Row[].
// node-api getRowObjectsJS() returns plain JS values (numbers/bigints/null).
function makeQueryFn(conn: Awaited<ReturnType<DuckDBInstance['connect']>>) {
  return async (sql: string): Promise<Row[]> => {
    const reader = await conn.runAndReadAll(sql)
    return reader.getRowObjectsJS() as unknown as Row[]
  }
}

// Each test gets a completely isolated in-memory DuckDB instance + connection.
// Sharing a single instance across tests causes data leakage between tests
// because all connections to the same :memory: instance share the same catalog.
async function makeConn() {
  const instance = await DuckDBInstance.create(':memory:')
  const conn = await instance.connect()
  const query = makeQueryFn(conn)
  await setupSchema(query)
  return { instance, conn, query }
}

// ── Schema setup ──────────────────────────────────────────────────────────────
describe('setupSchema', () => {
  it('creates the vl_battery_v table', async () => {
    const { query } = await makeConn()
    const cols = await query(
      `SELECT column_name FROM information_schema.columns WHERE table_name = 'vl_battery_v'`,
    ) as unknown as { column_name: string }[]
    const names = cols.map((r) => r.column_name)
    expect(names).toContain('timestamp')
    expect(names).toContain('received_timestamp')
    expect(names).toContain('value')
  })

  it('is idempotent — calling twice does not throw', async () => {
    const { query } = await makeConn()
    await expect(setupSchema(query)).resolves.not.toThrow()
  })
})

// ── insertTelemetrySQL ────────────────────────────────────────────────────────
describe('insertTelemetrySQL', () => {
  it('inserts a GPS point (timestamp not null)', async () => {
    const { query } = await makeConn()
    const ts = 1_700_000_000_000
    const rts = ts + 10
    await insertTelemetrySQL(query, TABLE, ts, rts, 0.5)

    const rows = await query(`SELECT * FROM ${TABLE}`)
    expect(rows).toHaveLength(1)
    expect(Number(rows[0].timestamp)).toBe(ts)
    expect(Number(rows[0].received_timestamp)).toBe(rts)
    expect(Number(rows[0].value)).toBeCloseTo(0.5)
  })

  it('inserts a received-only point (timestamp null)', async () => {
    const { query } = await makeConn()
    const rts = 1_700_000_001_000
    await insertTelemetrySQL(query, TABLE, null, rts, -0.3)

    const rows = await query(`SELECT * FROM ${TABLE}`)
    expect(rows).toHaveLength(1)
    expect(rows[0].timestamp).toBeNull()
    expect(Number(rows[0].received_timestamp)).toBe(rts)
  })
})

// ── getAllDataSQL ─────────────────────────────────────────────────────────────
describe('getAllDataSQL', () => {
  it('returns all rows sorted by received_timestamp ASC', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    await insertTelemetrySQL(query, TABLE, base + 200, base + 210, 0.2)
    await insertTelemetrySQL(query, TABLE, base + 100, base + 110, 0.1)
    await insertTelemetrySQL(query, TABLE, base + 300, base + 310, 0.3)

    const data = await getAllDataSQL(query)
    expect(data).toHaveLength(3)
    expect(data[0].receivedTimestamp).toBe(base + 110)
    expect(data[1].receivedTimestamp).toBe(base + 210)
    expect(data[2].receivedTimestamp).toBe(base + 310)
  })

  it('filters by afterReceivedTimestamp', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    await insertTelemetrySQL(query, TABLE, base + 100, base + 100, 0.1)
    await insertTelemetrySQL(query, TABLE, base + 200, base + 200, 0.2)
    await insertTelemetrySQL(query, TABLE, base + 300, base + 300, 0.3)

    const data = await getAllDataSQL(query, base + 150)
    expect(data).toHaveLength(2)
    expect(data[0].receivedTimestamp).toBe(base + 200)
    expect(data[1].receivedTimestamp).toBe(base + 300)
  })

  it('returns empty array when table is empty', async () => {
    const { query } = await makeConn()
    const data = await getAllDataSQL(query)
    expect(data).toEqual([])
  })
})

// ── queryTelemetrySQL — no strategy ──────────────────────────────────────────
describe('queryTelemetrySQL (no strategy)', () => {
  it('returns GPS points in range, ordered by timestamp ASC', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    await insertTelemetrySQL(query, TABLE, base + 100, base + 110, 0.1)
    await insertTelemetrySQL(query, TABLE, base + 200, base + 210, 0.2)
    await insertTelemetrySQL(query, TABLE, base + 300, base + 310, 0.3)
    // Out of range
    await insertTelemetrySQL(query, TABLE, base + 500, base + 510, 0.5)

    const data = await queryTelemetrySQL(query, TABLE, 'gps', base, base + 350)
    expect(data).toHaveLength(3)
    expect(data[0].timestamp).toBe(base + 100)
    expect(data[2].timestamp).toBe(base + 300)
  })

  it('returns received-only points (timestamp === null)', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    await insertTelemetrySQL(query, TABLE, null, base + 100, 0.1)
    await insertTelemetrySQL(query, TABLE, base + 200, base + 210, 0.2) // GPS — should be excluded

    const data = await queryTelemetrySQL(query, TABLE, 'received', base, base + 350)
    expect(data).toHaveLength(1)
    expect(data[0].timestamp).toBeNull()
    expect(data[0].receivedTimestamp).toBe(base + 100)
  })

  it('returns empty array when no data in range', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    await insertTelemetrySQL(query, TABLE, base + 100, base + 110, 0.1)

    const data = await queryTelemetrySQL(query, TABLE, 'gps', base + 500, base + 600)
    expect(data).toEqual([])
  })
})

// ── queryTelemetrySQL — latest strategy ──────────────────────────────────────
describe('queryTelemetrySQL (latest strategy)', () => {
  it('returns only the last N points in ascending order', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    for (let i = 0; i < 5; i++) {
      await insertTelemetrySQL(query, TABLE, base + i * 100, base + i * 100 + 5, i * 0.1)
    }

    const data = await queryTelemetrySQL(query, TABLE, 'gps', base, base + 1000, {
      strategy: 'latest',
      size: 2,
    })
    expect(data).toHaveLength(2)
    // Should be the last 2 points, in ascending order
    expect(data[0].timestamp).toBe(base + 300)
    expect(data[1].timestamp).toBe(base + 400)
  })

  it('returns all points when size exceeds count', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    await insertTelemetrySQL(query, TABLE, base + 100, base + 110, 0.1)
    await insertTelemetrySQL(query, TABLE, base + 200, base + 210, 0.2)

    const data = await queryTelemetrySQL(query, TABLE, 'gps', base, base + 1000, {
      strategy: 'latest',
      size: 10,
    })
    expect(data).toHaveLength(2)
  })
})

// ── queryTelemetrySQL — minmax strategy ──────────────────────────────────────
describe('queryTelemetrySQL (minmax strategy)', () => {
  it('returns min and max per bucket, non-empty, values in range', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    for (let i = 0; i < 20; i++) {
      const v = Math.sin((2 * Math.PI * i) / 20)
      await insertTelemetrySQL(query, TABLE, base + i * 100, base + i * 100 + 5, v)
    }

    const data = await queryTelemetrySQL(query, TABLE, 'gps', base, base + 2500, {
      strategy: 'minmax',
      size: 10,
    })
    expect(data.length).toBeGreaterThan(0)
    for (const d of data) {
      expect(d.value).toBeGreaterThanOrEqual(-1.1)
      expect(d.value).toBeLessThanOrEqual(1.1)
    }
  })

  it('returns results in ascending timestamp order', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    for (let i = 0; i < 10; i++) {
      await insertTelemetrySQL(query, TABLE, base + i * 100, base + i * 100 + 5, i * 0.1)
    }

    const data = await queryTelemetrySQL(query, TABLE, 'gps', base, base + 1500, {
      strategy: 'minmax',
      size: 6,
    })
    for (let i = 1; i < data.length; i++) {
      expect(data[i].timestamp!).toBeGreaterThanOrEqual(data[i - 1].timestamp!)
    }
  })

  it('returns empty array when no data in range', async () => {
    const { query } = await makeConn()
    const data = await queryTelemetrySQL(query, TABLE, 'gps', 1000, 2000, {
      strategy: 'minmax',
      size: 10,
    })
    expect(data).toEqual([])
  })
})

// ── minmax sparse data behaviour ─────────────────────────────────────────────
describe('queryTelemetrySQL (minmax sparse data)', () => {
  it('returns all original points when count < size (1 point per bucket, deduped)', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    // 5 points but request 100 buckets — should return all 5 originals
    const source = Array.from({ length: 5 }, (_, i) => ({
      ts: base + i * 1000,
      rts: base + i * 1000 + 5,
      value: i * 0.2,
    }))
    for (const s of source) {
      await insertTelemetrySQL(query, TABLE, s.ts, s.rts, s.value)
    }

    const data = await queryTelemetrySQL(query, TABLE, 'gps', base - 1000, base + 10_000, {
      strategy: 'minmax',
      size: 100,
    })

    expect(data).toHaveLength(5)
    for (let i = 0; i < 5; i++) {
      expect(data[i].timestamp).toBe(source[i].ts)
      expect(data[i].value).toBeCloseTo(source[i].value, 6)
    }
  })

  it('returns exactly 1 point when only 1 data point exists', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    await insertTelemetrySQL(query, TABLE, base + 500, base + 510, 0.42)

    const data = await queryTelemetrySQL(query, TABLE, 'gps', base, base + 5000, {
      strategy: 'minmax',
      size: 50,
    })

    expect(data).toHaveLength(1)
    expect(data[0].value).toBeCloseTo(0.42, 6)
  })

  it('returns up to 2*size points when data is dense', async () => {
    const { query } = await makeConn()
    const base = 1_700_000_000_000
    // 200 points with varying values, request size=10 → up to 20 results
    for (let i = 0; i < 200; i++) {
      const v = Math.sin((2 * Math.PI * i) / 200)
      await insertTelemetrySQL(query, TABLE, base + i * 10, base + i * 10 + 5, v)
    }

    const data = await queryTelemetrySQL(query, TABLE, 'gps', base, base + 3000, {
      strategy: 'minmax',
      size: 10,
    })

    // Each of the 10 buckets can emit up to 2 points (min + max)
    expect(data.length).toBeGreaterThan(10)
    expect(data.length).toBeLessThanOrEqual(20)
  })
})

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import {
  GenericContainer,
  type StartedTestContainer,
  Wait,
} from 'testcontainers'
import { DuckDBInstance } from '@duckdb/node-api'
import { InfluxDBBackend, type InfluxDBConfig } from './influxdb'
import {
  ensureTable,
  insertTelemetrySQL,
  getAllDataSQL,
  type Row,
} from './duckdb-store'

function makeQueryFn(conn: Awaited<ReturnType<DuckDBInstance['connect']>>) {
  return async (sql: string): Promise<Row[]> => {
    const reader = await conn.runAndReadAll(sql)
    return reader.getRowObjectsJS() as unknown as Row[]
  }
}

const KEY = 'vl_battery_v'
const ORG = 'testorg'
const BUCKET = 'testbucket'
const TOKEN = 'testtoken'

let container: StartedTestContainer
let config: InfluxDBConfig
let backend: InfluxDBBackend

beforeAll(async () => {
  container = await new GenericContainer('influxdb:2')
    .withExposedPorts(8086)
    .withEnvironment({
      DOCKER_INFLUXDB_INIT_MODE: 'setup',
      DOCKER_INFLUXDB_INIT_USERNAME: 'admin',
      DOCKER_INFLUXDB_INIT_PASSWORD: 'password123',
      DOCKER_INFLUXDB_INIT_ORG: ORG,
      DOCKER_INFLUXDB_INIT_BUCKET: BUCKET,
      DOCKER_INFLUXDB_INIT_ADMIN_TOKEN: TOKEN,
    })
    .withWaitStrategy(Wait.forHttp('/health', 8086).forStatusCode(200))
    .start()

  const host = container.getHost()
  const port = container.getMappedPort(8086)

  config = {
    url: `http://${host}:${port}`,
    token: TOKEN,
    org: ORG,
    bucket: BUCKET,
  }

  backend = new InfluxDBBackend(config)
  await backend.init()
})

afterAll(async () => {
  await container?.stop()
})

// Helper: wait for InfluxDB to make a just-written point queryable.
// InfluxDB has a small ingestion delay before data is visible to queries.
async function waitForData(
  key: string,
  start: number,
  end: number,
  expectedCount: number,
  retries = 10,
  delayMs = 300
): Promise<void> {
  for (let i = 0; i < retries; i++) {
    const data = await backend.queryTelemetry(key, start, end)
    if (data.length >= expectedCount) return
    await new Promise((r) => setTimeout(r, delayMs))
  }
}

// ── Raw CSV inspection ────────────────────────────────────────────────────────
// This test writes one point and dumps the raw Flux CSV so we can see exactly
// what InfluxDB returns — useful for debugging the parser.
describe('raw CSV inspection', () => {
  it('shows raw CSV from a query', async () => {
    const ts = Date.now()
    await backend.insertTelemetry(KEY, ts, 0.5)

    await waitForData(KEY, ts - 1000, ts + 2000, 1)

    const startRfc = new Date(ts - 1000).toISOString()
    const endRfc = new Date(ts + 2000).toISOString()

    const flux = `
from(bucket: "${BUCKET}")
  |> range(start: ${startRfc}, stop: ${endRfc})
  |> filter(fn: (r) => r._measurement == "${KEY}")
  |> filter(fn: (r) => r._field == "value")`

    const resp = await fetch(`${config.url}/api/v2/query?org=${ORG}`, {
      method: 'POST',
      headers: {
        Authorization: `Token ${TOKEN}`,
        'Content-Type': 'application/vnd.flux',
        Accept: 'application/csv',
      },
      body: flux,
    })

    const csv = await resp.text()
    console.log('=== RAW CSV ===\n', csv)
    expect(resp.ok).toBe(true)
  })
})

// ── Point roundtrip ──────────────────────────────────────────────────────────
describe('point roundtrip', () => {
  it('writes and reads back a point', async () => {
    const ts = Date.now() + 10_000
    const value = 0.123

    await backend.insertTelemetry(KEY, ts, value)
    await waitForData(KEY, ts - 1000, ts + 2000, 1)

    const results = await backend.queryTelemetry(KEY, ts - 1000, ts + 2000)

    expect(results.length).toBeGreaterThanOrEqual(1)
    const point = results.find((d) => Math.abs(d.timestampMs - ts) < 5)
    expect(point).toBeDefined()
    expect(point!.timestampMs).toBeCloseTo(ts, -1)
    expect(point!.value).toBeCloseTo(value, 5)
  })
})

// ── writeBatch roundtrip ──────────────────────────────────────────────────────
describe('writeBatch', () => {
  it('batch-writes multiple points and reads them all back in order', async () => {
    const base = Date.now() + 30_000
    const data = Array.from({ length: 5 }, (_, i) => ({
      timestampMs: base + i * 100,
      value: i * 0.1,
    }))

    await backend.writeBatch(KEY, data)
    await waitForData(KEY, base - 1000, base + 2000, 5)

    const results = await backend.queryTelemetry(KEY, base - 1000, base + 2000)

    expect(results.length).toBeGreaterThanOrEqual(5)
    // Results should be in ascending timestampMs order
    for (let i = 1; i < results.length; i++) {
      expect(results[i].timestampMs).toBeGreaterThanOrEqual(
        results[i - 1].timestampMs
      )
    }
  })
})

// ── latest strategy ───────────────────────────────────────────────────────────
describe('latest strategy', () => {
  it('returns only the most recent point when size=1', async () => {
    const base = Date.now() + 40_000
    const data = Array.from({ length: 3 }, (_, i) => ({
      timestampMs: base + i * 200,
      value: i * 0.2,
    }))

    await backend.writeBatch(KEY, data)
    await waitForData(KEY, base - 1000, base + 2000, 3)

    const results = await backend.queryTelemetry(
      KEY,
      base - 1000,
      base + 2000,
      {
        strategy: 'latest',
        size: 1,
      }
    )

    expect(results.length).toBe(1)
    expect(results[0].value).toBeCloseTo(0.4, 5)
  })
})

// ── minmax strategy ──────────────────────────────────────────────────────────
describe('minmax strategy', () => {
  it('returns aggregated (non-empty, reasonable) results', async () => {
    const base = Date.now() + 50_000
    const data = Array.from({ length: 20 }, (_, i) => ({
      timestampMs: base + i * 50,
      value: Math.sin((2 * Math.PI * i) / 20),
    }))

    await backend.writeBatch(KEY, data)
    await waitForData(KEY, base - 1000, base + 2000, 20)

    const results = await backend.queryTelemetry(
      KEY,
      base - 1000,
      base + 2000,
      {
        strategy: 'minmax',
        size: 10,
      }
    )

    expect(results.length).toBeGreaterThan(0)
    for (const r of results) {
      expect(r.value).toBeGreaterThanOrEqual(-1.1)
      expect(r.value).toBeLessThanOrEqual(1.1)
    }
  })
})

// ── minmax sparse data behaviour ──────────────────────────────────────────────
describe('minmax sparse data', () => {
  it('returns all original values when count < size (1 point per bucket, deduped)', async () => {
    const base = Date.now() + 60_000
    // 5 points but request 100 buckets — should return 5 values
    const source = Array.from({ length: 5 }, (_, i) => ({
      timestampMs: base + i * 1000,
      value: i * 0.2,
    }))

    await backend.writeBatch(KEY, source)
    await waitForData(KEY, base - 1000, base + 10_000, 5)

    const data = await backend.queryTelemetry(KEY, base - 1000, base + 10_000, {
      strategy: 'minmax',
      size: 100,
    })

    expect(data).toHaveLength(5)
    const sorted = [...data].sort((a, b) => a.timestampMs - b.timestampMs)
    for (let i = 0; i < 5; i++) {
      expect(sorted[i].timestampMs).toBeCloseTo(source[i].timestampMs, -1)
      expect(sorted[i].value).toBeCloseTo(source[i].value, 5)
    }
  })

  it('returns exactly 1 point when only 1 data point exists in range', async () => {
    const base = Date.now() + 70_000
    await backend.insertTelemetry(KEY, base + 500, 0.42)
    await waitForData(KEY, base, base + 5000, 1)

    const data = await backend.queryTelemetry(KEY, base, base + 5000, {
      strategy: 'minmax',
      size: 50,
    })

    expect(data).toHaveLength(1)
    expect(data[0].value).toBeCloseTo(0.42, 5)
  })

  it('returns up to 2*size points when data is dense', async () => {
    const base = Date.now() + 80_000
    // 200 points with varying values, request size=10 → up to 20 results
    const source = Array.from({ length: 200 }, (_, i) => ({
      timestampMs: base + i * 10,
      value: Math.sin((2 * Math.PI * i) / 200),
    }))

    await backend.writeBatch(KEY, source)
    await waitForData(KEY, base - 1000, base + 3000, 200)

    const data = await backend.queryTelemetry(KEY, base - 1000, base + 3000, {
      strategy: 'minmax',
      size: 10,
    })

    // Each of the 10 windows can emit up to 2 points (min + max)
    expect(data.length).toBeGreaterThan(10)
    expect(data.length).toBeLessThanOrEqual(20)
  })
})

// ── Empty range ───────────────────────────────────────────────────────────────
describe('empty range', () => {
  it('returns empty array when no data exists in range', async () => {
    // Use a time range far in the past with no data
    const start = new Date('2000-01-01').getTime()
    const end = new Date('2000-01-02').getTime()

    const results = await backend.queryTelemetry(KEY, start, end)
    expect(results).toEqual([])
  })
})

// ── DuckDB → InfluxDB end-to-end ──────────────────────────────────────────────
// Simulates the full upload flow: generate sine wave data into DuckDB,
// read it all back via getAllDataSQL, upload to InfluxDB via writeBatch,
// then query InfluxDB and verify the values match the originals.
describe('DuckDB → InfluxDB upload roundtrip', () => {
  it('values read from InfluxDB match values written from DuckDB', async () => {
    const POINT_COUNT = 40
    const base = Date.now() + 100_000

    // ── Step 1: write a sine wave into an in-memory DuckDB ──
    const duckInstance = await DuckDBInstance.create(':memory:')
    const duckConn = await duckInstance.connect()
    const duckQuery = makeQueryFn(duckConn)
    await ensureTable(duckQuery, KEY)

    const sourceData = Array.from({ length: POINT_COUNT }, (_, i) => ({
      timestampMs: base + i * 100,
      value: Math.sin((2 * Math.PI * i) / POINT_COUNT),
    }))

    for (const d of sourceData) {
      await insertTelemetrySQL(duckQuery, KEY, d.timestampMs, d.value)
    }

    // ── Step 2: read all data back from DuckDB ──
    const duckResult = await getAllDataSQL(duckQuery, [KEY])
    const duckData = duckResult.get(KEY)!
    expect(duckData).toHaveLength(POINT_COUNT)

    // ── Step 3: upload to InfluxDB ──
    await backend.writeBatch(KEY, duckData)
    await waitForData(
      KEY,
      base - 1000,
      base + POINT_COUNT * 100 + 1000,
      POINT_COUNT
    )

    // ── Step 4: query InfluxDB and compare ──
    const influxData = await backend.queryTelemetry(
      KEY,
      base - 1000,
      base + POINT_COUNT * 100 + 1000
    )

    console.log(
      `DuckDB points: ${duckData.length}, InfluxDB points: ${influxData.length}`
    )

    expect(influxData.length).toBe(POINT_COUNT)

    // Sort both by timestampMs for comparison
    const sorted = [...influxData].sort((a, b) => a.timestampMs - b.timestampMs)

    for (let i = 0; i < POINT_COUNT; i++) {
      const src = sourceData[i]
      const got = sorted[i]
      expect(got.timestampMs).toBeCloseTo(src.timestampMs, -1)
      expect(got.value).toBeCloseTo(src.value, 6)
    }

    duckInstance.closeSync()
  })
})

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { GenericContainer, type StartedTestContainer, Wait } from 'testcontainers'
import { InfluxDBBackend, type InfluxDBConfig } from './influxdb'

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
  series: 'gps' | 'received',
  start: number,
  end: number,
  expectedCount: number,
  retries = 10,
  delayMs = 300,
): Promise<void> {
  for (let i = 0; i < retries; i++) {
    const data = await backend.queryTelemetry('vl_battery_v', series, start, end)
    if (data.length >= expectedCount) return
    await new Promise((r) => setTimeout(r, delayMs))
  }
}

// ── Raw CSV inspection ────────────────────────────────────────────────────────
// This test writes one point and dumps the raw Flux CSV so we can see exactly
// what InfluxDB returns — useful for debugging the parser.
describe('raw CSV inspection', () => {
  it('shows raw CSV from a pivot query', async () => {
    const ts = Date.now()
    await backend.insertTelemetry('vl_battery_v', ts, ts + 1, 0.5)

    await waitForData('gps', ts - 1000, ts + 2000, 1)

    const startRfc = new Date(ts - 1000).toISOString()
    const endRfc = new Date(ts + 2000).toISOString()

    const flux = `
from(bucket: "${BUCKET}")
  |> range(start: ${startRfc}, stop: ${endRfc})
  |> filter(fn: (r) => r._measurement == "vl_battery_v" and r.series == "gps")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "value", "alt_ts"])`

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
    console.log('=== RAW PIVOT CSV ===\n', csv)
    expect(resp.ok).toBe(true)
  })
})

// ── GPS series roundtrip ──────────────────────────────────────────────────────
describe('GPS series roundtrip', () => {
  it('writes and reads back a GPS point', async () => {
    const gpsTsBase = Date.now() + 10_000
    const receivedTs = gpsTsBase + 50
    const value = 0.123

    await backend.insertTelemetry('vl_battery_v', gpsTsBase, receivedTs, value)
    await waitForData('gps', gpsTsBase - 1000, gpsTsBase + 2000, 1)

    const results = await backend.queryTelemetry(
      'vl_battery_v',
      'gps',
      gpsTsBase - 1000,
      gpsTsBase + 2000,
    )

    expect(results.length).toBeGreaterThanOrEqual(1)
    const point = results.find((d) => Math.abs((d.timestamp ?? 0) - gpsTsBase) < 5)
    expect(point).toBeDefined()
    expect(point!.timestamp).toBeCloseTo(gpsTsBase, -1)
    expect(point!.receivedTimestamp).toBeCloseTo(receivedTs, -1)
    expect(point!.value).toBeCloseTo(value, 5)
  })
})

// ── Received series roundtrip ─────────────────────────────────────────────────
describe('received series roundtrip', () => {
  it('writes and reads back a no-GPS point (timestamp === null)', async () => {
    const receivedTs = Date.now() + 20_000
    const value = -0.456

    await backend.insertTelemetry('vl_battery_v', null, receivedTs, value)
    await waitForData('received', receivedTs - 1000, receivedTs + 2000, 1)

    const results = await backend.queryTelemetry(
      'vl_battery_v',
      'received',
      receivedTs - 1000,
      receivedTs + 2000,
    )

    expect(results.length).toBeGreaterThanOrEqual(1)
    const point = results.find((d) => Math.abs(d.receivedTimestamp - receivedTs) < 5)
    expect(point).toBeDefined()
    expect(point!.timestamp).toBeNull()
    expect(point!.receivedTimestamp).toBeCloseTo(receivedTs, -1)
    expect(point!.value).toBeCloseTo(value, 5)
  })
})

// ── writeBatch roundtrip ──────────────────────────────────────────────────────
describe('writeBatch', () => {
  it('batch-writes multiple points and reads them all back in order', async () => {
    const base = Date.now() + 30_000
    const data = Array.from({ length: 5 }, (_, i) => ({
      timestamp: base + i * 100,
      receivedTimestamp: base + i * 100 + 10,
      value: i * 0.1,
    }))

    await backend.writeBatch(data)
    await waitForData('gps', base - 1000, base + 2000, 5)

    const results = await backend.queryTelemetry('vl_battery_v', 'gps', base - 1000, base + 2000)

    expect(results.length).toBeGreaterThanOrEqual(5)
    // Results should be in ascending timestamp order
    for (let i = 1; i < results.length; i++) {
      expect(results[i].timestamp!).toBeGreaterThanOrEqual(results[i - 1].timestamp!)
    }
  })
})

// ── latest strategy ───────────────────────────────────────────────────────────
describe('latest strategy', () => {
  it('returns only the most recent point when size=1', async () => {
    const base = Date.now() + 40_000
    const data = Array.from({ length: 3 }, (_, i) => ({
      timestamp: base + i * 200,
      receivedTimestamp: base + i * 200 + 5,
      value: i * 0.2,
    }))

    await backend.writeBatch(data)
    await waitForData('gps', base - 1000, base + 2000, 3)

    const results = await backend.queryTelemetry('vl_battery_v', 'gps', base - 1000, base + 2000, {
      strategy: 'latest',
      size: 1,
    })

    expect(results.length).toBe(1)
    expect(results[0].value).toBeCloseTo(0.4, 5)
  })
})

// ── minmax/mean strategy ──────────────────────────────────────────────────────
describe('minmax strategy', () => {
  it('returns aggregated (non-empty, reasonable) results', async () => {
    const base = Date.now() + 50_000
    const data = Array.from({ length: 20 }, (_, i) => ({
      timestamp: base + i * 50,
      receivedTimestamp: base + i * 50 + 5,
      value: Math.sin((2 * Math.PI * i) / 20),
    }))

    await backend.writeBatch(data)
    await waitForData('gps', base - 1000, base + 2000, 20)

    const results = await backend.queryTelemetry('vl_battery_v', 'gps', base - 1000, base + 2000, {
      strategy: 'minmax',
      size: 10,
    })

    expect(results.length).toBeGreaterThan(0)
    for (const r of results) {
      expect(r.value).toBeGreaterThanOrEqual(-1.1)
      expect(r.value).toBeLessThanOrEqual(1.1)
    }
  })
})

// ── Empty range ───────────────────────────────────────────────────────────────
describe('empty range', () => {
  it('returns empty array when no data exists in range', async () => {
    // Use a time range far in the past with no data
    const start = new Date('2000-01-01').getTime()
    const end = new Date('2000-01-02').getTime()

    const results = await backend.queryTelemetry('vl_battery_v', 'gps', start, end)
    expect(results).toEqual([])
  })
})

import { getConnection, insertTelemetry, queryTelemetry } from './duckdb'

const NAMESPACE = 'caduceus'
const SENSOR_KEY = 'vl_battery_v'
const TABLE_NAME = 'vl_battery_v'
const TICK_INTERVAL_MS = 100

type Callback = (datum: { timestamp: number; value: number }) => void

const subscribers = new Set<Callback>()
let generatorInterval: ReturnType<typeof setInterval> | null = null

function startGenerator() {
  if (generatorInterval !== null) return

  // Pre-warm the DuckDB connection so first inserts don't lag
  getConnection().catch(console.error)

  generatorInterval = setInterval(() => {
    const timestamp = Date.now()
    const value = Math.sin((2 * Math.PI * timestamp) / 10000)
    const datum = { timestamp, value }

    insertTelemetry(TABLE_NAME, timestamp, value).catch(console.error)

    for (const cb of subscribers) {
      cb(datum)
    }
  }, TICK_INTERVAL_MS)
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function VlBatteryPlugin(openmct: any) {
  openmct.types.addType('caduceus.telemetry', {
    name: 'Caduceus Telemetry Point',
    description: 'A telemetry measurement from the Caduceus system',
    cssClass: 'icon-telemetry',
  })

  openmct.objects.addRoot(
    { namespace: NAMESPACE, key: 'root' },
    openmct.priority.HIGH,
  )

  openmct.objects.addProvider(NAMESPACE, {
    get(identifier: { namespace: string; key: string }) {
      if (identifier.key === 'root') {
        return Promise.resolve({
          identifier,
          name: 'Caduceus',
          type: 'folder',
          location: 'ROOT',
          composition: [{ namespace: NAMESPACE, key: SENSOR_KEY }],
        })
      }

      if (identifier.key === SENSOR_KEY) {
        return Promise.resolve({
          identifier,
          name: 'VL Battery Voltage',
          type: 'caduceus.telemetry',
          location: `${NAMESPACE}:root`,
          telemetry: {
            values: [
              {
                key: 'value',
                name: 'Voltage',
                unit: 'V',
                format: 'float',
                min: -1,
                max: 1,
                hints: { range: 1 },
              },
              {
                key: 'utc',
                source: 'timestamp',
                name: 'Timestamp',
                format: 'utc',
                hints: { domain: 1 },
              },
            ],
          },
        })
      }

      return Promise.resolve(undefined)
    },
  })

  openmct.telemetry.addProvider({
    supportsRequest(domainObject: { identifier: { namespace: string } }) {
      return domainObject.identifier.namespace === NAMESPACE
    },

    async request(
      domainObject: { identifier: { key: string } },
      options: { start: number; end: number },
    ) {
      if (domainObject.identifier.key !== SENSOR_KEY) return []
      return queryTelemetry(TABLE_NAME, options.start, options.end)
    },

    supportsSubscribe(domainObject: { identifier: { namespace: string; key: string } }) {
      return (
        domainObject.identifier.namespace === NAMESPACE &&
        domainObject.identifier.key === SENSOR_KEY
      )
    },

    subscribe(
      _domainObject: unknown,
      callback: Callback,
    ): () => void {
      subscribers.add(callback)
      startGenerator()
      return () => {
        subscribers.delete(callback)
      }
    },
  })
}

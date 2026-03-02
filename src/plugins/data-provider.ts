import { getBackend, getBackendType } from '../db/get-backend'
import type { QueryOptions } from '../db/backend'

export type Datum = {
  key: string
  value: number
  /** Unix timestamp in ms */
  timestampMs: number
}

export type DataKey = {
  key: string
  /**
   * Minimum x-axis gap (ms) treated as a data break in plots.
   * 0 disables gap detection.
   */
  gapThreshold: number
}

export interface DataSource {
  /**
   * Returns every key this source may emit, with per-key plot metadata.
   * @returns array of data key descriptors
   */
  allKeys(): DataKey[]

  /**
   * Start producing data. Calls `onData` for each new datum.
   * @param onData callback invoked for every new datum
   */
  subscribe(onData: (data: Datum) => void): void
}

export const NAMESPACE = 'nick'

type OpenMCTDatum = {
  utc: number
  value: number
}

/** Build per-key telemetry value descriptors, embedding gapThreshold on the range value. */
function telemetryValues(dataKey: DataKey) {
  return [
    {
      key: 'value',
      name: 'Value',
      format: 'float',
      hints: { range: 1 },
      gapThreshold: dataKey.gapThreshold,
    },
    { key: 'utc', name: 'Timestamp', format: 'utc', hints: { domain: 1 } },
  ]
}

function toOpenMCTDatum(d: {
  timestampMs: number
  value: number
}): OpenMCTDatum {
  return { utc: d.timestampMs, value: d.value }
}

/** Keys eagerly registered via {@link registerDataSource}. */
const registeredKeys = new Map<string, DataKey>()

/** Per-key subscriber sets for OpenMCT realtime subscriptions. */
const keySubscribers = new Map<string, Set<(datum: OpenMCTDatum) => void>>()

/**
 * Registers a data source's keys so OpenMCT can resolve their objects,
 * and starts its subscription to insert and forward live data.
 *
 * @param source  the data source to register
 */
export function registerDataSource(source: DataSource): void {
  for (const dataKey of source.allKeys()) {
    registeredKeys.set(dataKey.key, dataKey)
  }

  source.subscribe((datum: Datum) => {
    getBackend()
      .then((backend) =>
        backend.insertTelemetry(datum.key, datum.timestampMs, datum.value)
      )
      .catch(console.error)

    const subscribers = keySubscribers.get(datum.key)
    if (subscribers) {
      const mapped = toOpenMCTDatum(datum)
      for (const cb of subscribers) {
        cb(mapped)
      }
    }
  })
}

/**
 * Registers keys so OpenMCT can resolve their objects without starting
 * a live data subscription. Use this when the backend does not produce
 * local data (e.g. InfluxDB) but the layout still references these keys.
 *
 * @param keys  the data key descriptors to register
 */
export function registerDataSourceKeys(keys: DataKey[]): void {
  for (const dataKey of keys) {
    registeredKeys.set(dataKey.key, dataKey)
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function DataProviderPlugin(openmct: any) {
  openmct.types.addType(`${NAMESPACE}.telemetry`, {
    name: 'NICK Telemetry Point',
    description: 'A telemetry measurement from the NICK',
    cssClass: 'icon-telemetry',
  })

  openmct.objects.addProvider(NAMESPACE, {
    get(identifier: { namespace: string; key: string }) {
      const dataKey = registeredKeys.get(identifier.key)
      if (!dataKey) return Promise.resolve(undefined)
      return Promise.resolve({
        identifier,
        name: identifier.key,
        type: `${NAMESPACE}.telemetry`,
        location: `${NAMESPACE}:root`,
        telemetry: { values: telemetryValues(dataKey) },
      })
    },
  })

  openmct.telemetry.addProvider({
    supportsRequest(domainObject: { identifier: { namespace: string } }) {
      return domainObject.identifier.namespace === NAMESPACE
    },

    async request(
      domainObject: { identifier: { key: string } },
      options: { start: number; end: number; strategy?: string; size?: number }
    ) {
      const key = domainObject.identifier.key
      const queryOpts: QueryOptions = {}
      if (options.strategy === 'minmax' || options.strategy === 'latest') {
        queryOpts.strategy = options.strategy
      }
      if (options.size) {
        queryOpts.size = options.size
      }
      const backend = await getBackend()
      const data = await backend.queryTelemetry(
        key,
        options.start,
        options.end,
        queryOpts
      )
      return data.map(toOpenMCTDatum)
    },

    supportsSubscribe(domainObject: { identifier: { namespace: string } }) {
      if (getBackendType() === 'influxdb') return false
      return domainObject.identifier.namespace === NAMESPACE
    },

    subscribe(
      domainObject: { identifier: { key: string } },
      callback: (datum: OpenMCTDatum) => void
    ): () => void {
      const key = domainObject.identifier.key
      if (!keySubscribers.has(key)) {
        keySubscribers.set(key, new Set())
      }
      // Always present — just ensured above
      const subscribers = keySubscribers.get(key)!
      subscribers.add(callback)
      return () => {
        subscribers.delete(callback)
      }
    },
  })
}

import { getBackend, getBackendType } from '../db/get-backend'
import type { QueryOptions } from '../db/backend'

export type Datum = {
  key: string
  value: number
  /** Unix timestamp in ms */
  timestampMs: number
}

export interface DataSource {
  /**
   * Returns every key this source may emit.
   * @returns array of datum keys
   */
  allKeys(): string[]

  /**
   * Start producing data. Calls `onData` for each new datum.
   * @param onData callback invoked for every new datum
   */
  subscribe(onData: (data: Datum) => void): void
}

export const NAMESPACE = 'caduceus'

type OpenMCTDatum = {
  utc: number
  value: number
}

/** Telemetry value descriptors used by the object provider. */
const TELEMETRY_VALUES = [
  { key: 'value', name: 'Value', format: 'float', hints: { range: 1 } },
  { key: 'utc', name: 'Timestamp', format: 'utc', hints: { domain: 1 } },
]

function toOpenMCTDatum(d: {
  timestampMs: number
  value: number
}): OpenMCTDatum {
  return { utc: d.timestampMs, value: d.value }
}

/** Keys eagerly registered via {@link registerDataSource}. */
const registeredKeys = new Set<string>()

/** Per-key subscriber sets for OpenMCT realtime subscriptions. */
const keySubscribers = new Map<string, Set<(datum: OpenMCTDatum) => void>>()

/**
 * Registers a data source: records its keys and starts its subscription.
 *
 * @param source  the data source to register
 */
export function registerDataSource(source: DataSource): void {
  for (const key of source.allKeys()) {
    registeredKeys.add(key)
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function DataProviderPlugin(openmct: any) {
  openmct.objects.addProvider(NAMESPACE, {
    get(identifier: { namespace: string; key: string }) {
      if (!registeredKeys.has(identifier.key)) return Promise.resolve(undefined)
      return Promise.resolve({
        identifier,
        name: identifier.key,
        type: 'caduceus.telemetry',
        location: `${NAMESPACE}:root`,
        telemetry: { values: TELEMETRY_VALUES },
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

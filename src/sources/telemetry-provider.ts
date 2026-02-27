import { getBackend, getBackendType } from '../db/get-backend'
import type { QueryOptions, TelemetrySeries } from '../db/backend'
import { subscribe, type Callback } from './fake-data-generator'

const NAMESPACE = 'caduceus'
const TABLE_NAME = 'vl_battery_v'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function TelemetryProviderPlugin(openmct: any) {
  openmct.telemetry.addProvider({
    supportsRequest(domainObject: { identifier: { namespace: string; key: string } }) {
      return (
        domainObject.identifier.namespace === NAMESPACE &&
        (domainObject.identifier.key === 'vl_battery_v_gps' ||
          domainObject.identifier.key === 'vl_battery_v_received')
      )
    },

    async request(
      domainObject: { identifier: { key: string } },
      options: { start: number; end: number; strategy?: string; size?: number },
    ) {
      const series: TelemetrySeries =
        domainObject.identifier.key === 'vl_battery_v_gps' ? 'gps' : 'received'
      const queryOpts: QueryOptions = {}
      if (options.strategy === 'minmax' || options.strategy === 'latest') {
        queryOpts.strategy = options.strategy
      }
      if (options.size) {
        queryOpts.size = options.size
      }
      const backend = await getBackend()
      return backend.queryTelemetry(TABLE_NAME, series, options.start, options.end, queryOpts)
    },

    supportsSubscribe(domainObject: { identifier: { namespace: string; key: string } }) {
      if (getBackendType() === 'influxdb') return false
      return (
        domainObject.identifier.namespace === NAMESPACE &&
        (domainObject.identifier.key === 'vl_battery_v_gps' ||
          domainObject.identifier.key === 'vl_battery_v_received')
      )
    },

    subscribe(
      domainObject: { identifier: { key: string } },
      callback: Callback,
    ): () => void {
      const series: TelemetrySeries =
        domainObject.identifier.key === 'vl_battery_v_gps' ? 'gps' : 'received'

      const filtered: Callback = (datum) => {
        if (series === 'gps' && datum.timestamp === null) return
        if (series === 'received' && datum.timestamp !== null) return
        callback(datum)
      }

      return subscribe(filtered)
    },
  })
}

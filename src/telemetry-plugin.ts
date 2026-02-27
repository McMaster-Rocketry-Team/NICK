import {
  getConnection,
  insertTelemetry,
  queryTelemetry,
  type QueryOptions,
  type TelemetrySeries,
} from './duckdb'

const NAMESPACE = 'caduceus'
const TABLE_NAME = 'vl_battery_v'
const TICK_INTERVAL_MS = 10

type Datum = { timestamp: number | null; receivedTimestamp: number; value: number }
type Callback = (datum: Datum) => void

const subscribers = new Set<Callback>()
let generatorInterval: ReturnType<typeof setInterval> | null = null
let hasGpsFix = true

function startGenerator() {
  if (generatorInterval !== null) return

  getConnection().catch(console.error)

  // Toggle GPS fix on/off every 2 seconds
  setInterval(() => {
    hasGpsFix = !hasGpsFix
  }, 2000)

  generatorInterval = setInterval(() => {
    const receivedTimestamp = Date.now()
    const timestamp = hasGpsFix ? receivedTimestamp : null
    const value = Math.sin((2 * Math.PI * receivedTimestamp) / 10000)
    const datum: Datum = { timestamp, receivedTimestamp, value }

    insertTelemetry(TABLE_NAME, timestamp, receivedTimestamp, value).catch(console.error)

    for (const cb of subscribers) {
      cb(datum)
    }
  }, TICK_INTERVAL_MS)
}

function makeTelemetryObject(series: TelemetrySeries) {
  const isGps = series === 'gps'
  return {
    identifier: { namespace: NAMESPACE, key: `vl_battery_v_${series}` },
    name: isGps ? 'VL Battery Voltage (GPS)' : 'VL Battery Voltage (Received)',
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
          source: isGps ? 'timestamp' : 'receivedTimestamp',
          name: isGps ? 'GPS Timestamp' : 'Received Timestamp',
          format: 'utc',
          hints: { domain: 1 },
        },
      ],
    },
  }
}

const OVERLAY_PLOT_KEY = 'vl_battery_overlay'
const LAYOUT_KEY = 'layout'

// Stable IDs for containers and frames so OpenMCT doesn't recreate them on each load
const CONTAINER_TOP_ID = 'c0000000-0000-0000-0000-000000000001'
const CONTAINER_BOTTOM_ID = 'c0000000-0000-0000-0000-000000000002'
const FRAME_PLOT_ID = 'f0000000-0000-0000-0000-000000000001'

function makeOverlayPlot() {
  return {
    identifier: { namespace: NAMESPACE, key: OVERLAY_PLOT_KEY },
    name: 'VL Battery Voltage',
    type: 'telemetry.plot.overlay',
    location: `${NAMESPACE}:${LAYOUT_KEY}`,
    composition: [
      { namespace: NAMESPACE, key: 'vl_battery_v_received' },
      { namespace: NAMESPACE, key: 'vl_battery_v_gps' },
    ],
    configuration: {
      series: [
        { identifier: { namespace: NAMESPACE, key: 'vl_battery_v_received' } },
        { identifier: { namespace: NAMESPACE, key: 'vl_battery_v_gps' } },
      ],
      objectStyles: {},
      legend: {
        position: 'top',
        expandByDefault: false,
        hideLegendWhenSmall: false,
        showTimestampWhenExpanded: true,
        showValueWhenExpanded: true,
        showMinimumWhenExpanded: true,
        showMaximumWhenExpanded: true,
        showUnitsWhenExpanded: true,
      },
    },
  }
}

function makeLayout() {
  return {
    identifier: { namespace: NAMESPACE, key: LAYOUT_KEY },
    name: 'Caduceus Dashboard',
    type: 'flexible-layout',
    location: `${NAMESPACE}:root`,
    composition: [
      { namespace: NAMESPACE, key: OVERLAY_PLOT_KEY },
    ],
    configuration: {
      rowsLayout: true,
      containers: [
        {
          id: CONTAINER_TOP_ID,
          size: 50,
          frames: [
            {
              id: FRAME_PLOT_ID,
              domainObjectIdentifier: { namespace: NAMESPACE, key: OVERLAY_PLOT_KEY },
              noFrame: false,
            },
          ],
        },
        {
          id: CONTAINER_BOTTOM_ID,
          size: 50,
          frames: [],
        },
      ],
      objectStyles: {},
    },
  }
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
          composition: [
            { namespace: NAMESPACE, key: LAYOUT_KEY },
            { namespace: NAMESPACE, key: 'vl_battery_v_gps' },
            { namespace: NAMESPACE, key: 'vl_battery_v_received' },
          ],
        })
      }

      if (identifier.key === LAYOUT_KEY) {
        return Promise.resolve(makeLayout())
      }

      if (identifier.key === OVERLAY_PLOT_KEY) {
        return Promise.resolve(makeOverlayPlot())
      }

      if (identifier.key === 'vl_battery_v_gps') {
        return Promise.resolve(makeTelemetryObject('gps'))
      }

      if (identifier.key === 'vl_battery_v_received') {
        return Promise.resolve(makeTelemetryObject('received'))
      }

      return Promise.resolve(undefined)
    },
  })

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
      const series: TelemetrySeries = domainObject.identifier.key === 'vl_battery_v_gps'
        ? 'gps'
        : 'received'
      const queryOpts: QueryOptions = {}
      if (options.strategy === 'minmax' || options.strategy === 'latest') {
        queryOpts.strategy = options.strategy
      }
      if (options.size) {
        queryOpts.size = options.size
      }
      return queryTelemetry(TABLE_NAME, series, options.start, options.end, queryOpts)
    },

    supportsSubscribe(domainObject: { identifier: { namespace: string; key: string } }) {
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
      const series: TelemetrySeries = domainObject.identifier.key === 'vl_battery_v_gps'
        ? 'gps'
        : 'received'

      const filtered: Callback = (datum) => {
        if (series === 'gps' && datum.timestamp === null) return
        if (series === 'received' && datum.timestamp !== null) return
        callback(datum)
      }

      subscribers.add(filtered)
      startGenerator()
      return () => {
        subscribers.delete(filtered)
      }
    },
  })
}

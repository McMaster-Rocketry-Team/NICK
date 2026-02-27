import type { TelemetrySeries } from '../db/backend'

const NAMESPACE = 'caduceus'

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
export const DATA_SOURCE_SWITCHER_KEY = 'data-source-switcher'

// Stable IDs for containers and frames so OpenMCT doesn't recreate them on each load
const CONTAINER_TOP_ID = 'c0000000-0000-0000-0000-000000000001'
const CONTAINER_BOTTOM_ID = 'c0000000-0000-0000-0000-000000000002'
const FRAME_PLOT_ID = 'f0000000-0000-0000-0000-000000000001'
const FRAME_SWITCHER_ID = 'f0000000-0000-0000-0000-000000000002'

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
      yAxis: {
        autoscale: false,
        range: { min: -1.5, max: 1.5 },
      },
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
      { namespace: NAMESPACE, key: DATA_SOURCE_SWITCHER_KEY },
    ],
    configuration: {
      rowsLayout: true,
      containers: [
        {
          id: CONTAINER_TOP_ID,
          size: 80,
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
          size: 20,
          frames: [
            {
              id: FRAME_SWITCHER_ID,
              domainObjectIdentifier: {
                namespace: NAMESPACE,
                key: DATA_SOURCE_SWITCHER_KEY,
              },
              noFrame: false,
            },
          ],
        },
      ],
      objectStyles: {},
    },
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function AvionicsLayoutPlugin(openmct: any) {
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
            { namespace: NAMESPACE, key: DATA_SOURCE_SWITCHER_KEY },
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

      if (identifier.key === DATA_SOURCE_SWITCHER_KEY) {
        return Promise.resolve({
          identifier,
          name: 'Data Source Switcher',
          type: 'caduceus.data-source-switcher',
          location: `${NAMESPACE}:${LAYOUT_KEY}`,
        })
      }

      return Promise.resolve(undefined)
    },
  })
}

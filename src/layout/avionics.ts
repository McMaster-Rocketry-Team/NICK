import {
  VL_BATTERY_GPS,
  VL_BATTERY_RECEIVED,
} from '../sources/fake-data-generator'
import { NAMESPACE } from '../plugins/data-provider'
import { UPLINK_PANEL_TYPE } from '../plugins/uplink-panel'
import type { OpenMCT } from 'openmct'
import { UPLINK_CONSOLE_NAMESPACE, UPLINK_CONSOLE_ROOT_KEY, UPLINK_CONSOLE_TYPE } from '../plugins/uplink-console'

export const AVIONICS_NAMESPACE = 'avionics'

const OVERLAY_PLOT_KEY = 'vl_battery_overlay'
const ALT_VEL_OVERLAY_KEY = 'alt_vel_overlay'
const LAYOUT_KEY = 'layout'
const RSSI_DISPLAY_KEY = 'gsd_rssi_display'
export const DATA_SOURCE_SWITCHER_KEY = 'data-source-switcher'
export const UPLINK_PANEL_KEY = 'uplink-panel'

// Stable IDs for containers and frames so OpenMCT doesn't recreate them on each load
const CONTAINER_TOP_ID = 'c0000000-0000-0000-0000-000000000001'
const CONTAINER_BOTTOM_ID = 'c0000000-0000-0000-0000-000000000002'
const FRAME_PLOT_ID = 'f0000000-0000-0000-0000-000000000001'
const FRAME_SWITCHER_ID = 'f0000000-0000-0000-0000-000000000002'
const FRAME_UPLINK_ID = 'f0000000-0000-0000-0000-000000000003'
const FRAME_RSSI_ID = 'f0000000-0000-0000-0000-000000000004'

function makeAltVelOverlayPlot() {
  return {
    identifier: { namespace: AVIONICS_NAMESPACE, key: ALT_VEL_OVERLAY_KEY },
    name: 'Altitude and Velocity',
    type: 'telemetry.plot.overlay',
    location: `${AVIONICS_NAMESPACE}:${LAYOUT_KEY}`,
    composition: [
      { namespace: NAMESPACE, key: 'gsd_altitude_agl' },
      { namespace: NAMESPACE, key: 'gsd_air_speed' },
    ],
    configuration: {
      series: [
        {
          identifier: { namespace: NAMESPACE, key: 'gsd_altitude_agl' },
        },
        {
          identifier: { namespace: NAMESPACE, key: 'gsd_air_speed' },
        },
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

function makeRssiDisplayLayout() {
  return {
    identifier: { namespace: AVIONICS_NAMESPACE, key: RSSI_DISPLAY_KEY },
    name: 'GSD RSSI',
    type: 'layout',
    location: `${AVIONICS_NAMESPACE}:${LAYOUT_KEY}`,
    composition: [{ namespace: NAMESPACE, key: 'gsd_rssi' }],
    configuration: {
      items: [
        {
          id: 'rssi-alphanumeric-item',
          type: 'telemetry-view',
          identifier: { namespace: NAMESPACE, key: 'gsd_rssi' },
          x: 1,
          y: 1,
          width: 12,
          height: 4,
          stroke: 'transparent',
          fill: 'transparent',
          fontSize: 'large',
          font: 'default',
          displayMode: 'all',
          showFrame: false,
        },
      ],
      layoutGrid: [10, 10],
    },
  }
}

function makeOverlayPlot() {
  return {
    identifier: { namespace: AVIONICS_NAMESPACE, key: OVERLAY_PLOT_KEY },
    name: 'VL Battery Voltage',
    type: 'telemetry.plot.overlay',
    location: `${AVIONICS_NAMESPACE}:${LAYOUT_KEY}`,
    composition: [
      { namespace: NAMESPACE, key: VL_BATTERY_GPS.key },
      { namespace: NAMESPACE, key: VL_BATTERY_RECEIVED.key },
    ],
    configuration: {
      series: [
        {
          identifier: { namespace: NAMESPACE, key: VL_BATTERY_RECEIVED.key },
        },
        {
          identifier: { namespace: NAMESPACE, key: VL_BATTERY_GPS.key },
        },
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
    identifier: { namespace: AVIONICS_NAMESPACE, key: LAYOUT_KEY },
    name: 'Avionics Dashboard',
    type: 'flexible-layout',
    location: 'ROOT',
    composition: [
      { namespace: AVIONICS_NAMESPACE, key: ALT_VEL_OVERLAY_KEY },
      // { namespace: AVIONICS_NAMESPACE, key: UPLINK_PANEL_KEY },
      { namespace: UPLINK_CONSOLE_NAMESPACE, key: UPLINK_CONSOLE_ROOT_KEY},
      { namespace: AVIONICS_NAMESPACE, key: DATA_SOURCE_SWITCHER_KEY },
      { namespace: AVIONICS_NAMESPACE, key: RSSI_DISPLAY_KEY },
    ],
    configuration: {
      rowsLayout: true,
      containers: [
        {
          id: CONTAINER_TOP_ID,
          size: 55,
          frames: [
            {
              id: FRAME_PLOT_ID,
              domainObjectIdentifier: {
                namespace: AVIONICS_NAMESPACE,
                key: ALT_VEL_OVERLAY_KEY,
              },
              noFrame: false,
            },
            {
              id: FRAME_RSSI_ID,
              domainObjectIdentifier: {
                namespace: AVIONICS_NAMESPACE,
                key: RSSI_DISPLAY_KEY,
              },
              noFrame: false,
            },
          ],
        },
        {
          id: CONTAINER_BOTTOM_ID,
          size: 45,
          frames: [
            {
              id: FRAME_UPLINK_ID,
              domainObjectIdentifier: {
                namespace: UPLINK_CONSOLE_NAMESPACE,
                key: UPLINK_CONSOLE_ROOT_KEY,
              },
              noFrame: false,
            },
            {
              id: FRAME_SWITCHER_ID,
              domainObjectIdentifier: {
                namespace: AVIONICS_NAMESPACE,
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

export function AvionicsLayoutPlugin(openmct: OpenMCT) {
  openmct.objects.addRoot(
    { namespace: AVIONICS_NAMESPACE, key: LAYOUT_KEY },
    openmct.priority.HIGH
  )

  openmct.objects.addProvider(AVIONICS_NAMESPACE, {
    get(identifier: { namespace: string; key: string }) {
      if (identifier.key === LAYOUT_KEY) {
        return Promise.resolve(makeLayout())
      }

      if (identifier.key === OVERLAY_PLOT_KEY) {
        return Promise.resolve(makeOverlayPlot())
      }

      if (identifier.key == ALT_VEL_OVERLAY_KEY) {
        return Promise.resolve(makeAltVelOverlayPlot())
      }

      if (identifier.key === RSSI_DISPLAY_KEY) {
        return Promise.resolve(makeRssiDisplayLayout())
      }

      if (identifier.key === DATA_SOURCE_SWITCHER_KEY) {
        return Promise.resolve({
          identifier,
          name: 'Data Source Switcher',
          type: `${NAMESPACE}.data-source-switcher`,
          location: `${AVIONICS_NAMESPACE}:${LAYOUT_KEY}`,
        })
      }

      if (identifier.key === UPLINK_PANEL_KEY) {
        return Promise.resolve({
          identifier,
          name: 'Send Uplink',
          type: UPLINK_PANEL_TYPE,
          location: `${AVIONICS_NAMESPACE}:${LAYOUT_KEY}`,
        })
      }

      return Promise.resolve(undefined)
    },
  })
}

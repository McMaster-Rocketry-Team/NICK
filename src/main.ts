import { registerSW } from 'virtual:pwa-register'
import openmct from 'openmct'
import snowThemeUrl from '../vendor/openmct/dist/snowTheme.css?url'
import espressoThemeUrl from '../vendor/openmct/dist/espressoTheme.css?url'
import { AvionicsLayoutPlugin } from './layout/avionics'
import {
  DataProviderPlugin,
  registerDataSource,
  registerDataSourceKeys,
} from './plugins/data-provider'
import { DataSourceSwitcherPlugin } from './plugins/data-source-switcher'
import { UplinkPanelPlugin } from './plugins/uplink-panel'
import { UplinkConsolePlugin } from './plugins/uplink-console'
import { DownlinkConsolePlugin } from './plugins/downlink-console'
import { DownlinkConditionSetsPlugin } from './plugins/downlink-condition-sets'
import { RocketTelemetryTreePlugin } from './plugins/rocket-telemetry-tree'
import { FakeDataGenerator } from './sources/fake-data-generator'
import { GroundStationSource, RocketTelemetryGenerator } from './sources/rocket-telemetry'
import { getBackendType } from './db/get-backend'

// service worker for PWA
const updateSW = registerSW({
  immediate: true,
  onNeedRefresh() {
    // New version available — reload immediately to activate it
    updateSW(true)
  },
})

// dynamically load openmct theme
const themeLink = document.createElement('link')
themeLink.rel = 'stylesheet'
const darkMq = window.matchMedia('(prefers-color-scheme: dark)')
themeLink.href = darkMq.matches ? espressoThemeUrl : snowThemeUrl
document.head.appendChild(themeLink)
darkMq.addEventListener('change', (e) => {
  themeLink.href = e.matches ? espressoThemeUrl : snowThemeUrl
})

// install openmct plugins
openmct.install(openmct.plugins.LocalStorage())
openmct.install(openmct.plugins.UTCTimeSystem())
openmct.install(openmct.plugins.Clock({ enableClockIndicator: true }))
openmct.install(
  openmct.plugins.DisplayLayout({ showAsView: ['summary-widget'] })
)
openmct.install(openmct.plugins.Notebook())
openmct.install(openmct.plugins.Condition())
// Priority below the Downlink Condition Sets folder (which uses priority.LOW)
// so "My Items" sorts beneath it in the tree. Root order is by priority, not
// install order. First two args are MyItems' own defaults (root name, namespace).
openmct.install(
  openmct.plugins.MyItems('My Items', '', openmct.priority.LOW - 1)
)
openmct.install(
  openmct.plugins.Conductor({
    menuOptions: [
      {
        clock: 'local',
        timeSystem: 'utc',
        clockOffsets: { start: -(5 * 60 * 1000), end: 0 },
        zoomOutLimit: 365 * 24 * 60 * 60 * 1000,
        zoomInLimit: 1000,
      },
      {
        timeSystem: 'utc',
        bounds: { start: Date.now() - 5 * 60 * 1000, end: Date.now() },
        zoomOutLimit: 365 * 24 * 60 * 60 * 1000,
        zoomInLimit: 1000,
      },
    ],
  })
)

openmct.install(DataProviderPlugin)
openmct.install(DataSourceSwitcherPlugin)
openmct.install(UplinkPanelPlugin)
openmct.install(UplinkConsolePlugin)
openmct.install(DownlinkConsolePlugin)
openmct.install(DownlinkConditionSetsPlugin)

// register layouts here
openmct.install(AvionicsLayoutPlugin)
openmct.install(RocketTelemetryTreePlugin)

// register data sources here
if (getBackendType() === 'duckdb') {
  registerDataSource(new FakeDataGenerator())
  // registerDataSource(new RocketTelemetryGenerator())
  registerDataSource(new GroundStationSource());
} else {
  registerDataSourceKeys(FakeDataGenerator.allKeys())
  // registerDataSourceKeys(RocketTelemetryGenerator.allKeys())
  registerDataSourceKeys(GroundStationSource.allKeys())
}

openmct.time.setTimeSystem('utc', {
  start: Date.now() - 5 * 60 * 1000,
  end: Date.now(),
})

if (
  !window.location.hash ||
  window.location.hash === '#/' ||
  window.location.hash === '#/browse/'
) {
  window.location.hash = '#/browse/avionics:layout'
}

openmct.start()

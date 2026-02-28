import { registerSW } from 'virtual:pwa-register'
import openmct from 'openmct'
import './main.css'
import snowThemeUrl from '../vendor/openmct/dist/snowTheme.css?url'
import espressoThemeUrl from '../vendor/openmct/dist/espressoTheme.css?url'

const themeLink = document.createElement('link')
themeLink.rel = 'stylesheet'
const darkMq = window.matchMedia('(prefers-color-scheme: dark)')
themeLink.href = darkMq.matches ? espressoThemeUrl : snowThemeUrl
document.head.appendChild(themeLink)
darkMq.addEventListener('change', (e) => {
  themeLink.href = e.matches ? espressoThemeUrl : snowThemeUrl
})

const updateSW = registerSW({
  immediate: true,
  onNeedRefresh() {
    // New version available — reload immediately to activate it
    updateSW(true)
  },
})
import { AvionicsLayoutPlugin } from './layout/avionics'
import {
  DataProviderPlugin,
  registerDataSource,
  registerDataSourceKeys,
} from './plugins/data-provider'
import { DataSourceSwitcherPlugin } from './plugins/data-source-switcher'
import { FakeDataGenerator } from './sources/fake-data-generator'
import { getBackendType } from './db/get-backend'

openmct.install(openmct.plugins.LocalStorage())
openmct.install(openmct.plugins.MyItems())
openmct.install(openmct.plugins.UTCTimeSystem())
openmct.install(openmct.plugins.Clock({ enableClockIndicator: true }))
openmct.install(openmct.plugins.Snow())
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

// register layouts here
openmct.install(AvionicsLayoutPlugin)

// register data sources here
if (getBackendType() === 'duckdb') {
  registerDataSource(new FakeDataGenerator())
} else {
  registerDataSourceKeys(FakeDataGenerator.allKeys())
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

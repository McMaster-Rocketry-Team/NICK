import openmct from 'openmct'
import { AvionicsLayoutPlugin } from './layout/avionics'
import { TelemetryProviderPlugin } from './sources/telemetry-provider'
import { DataSourceSwitcherPlugin } from './components/data-source-switcher'

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
        clockOffsets: { start: -(15 * 60 * 1000), end: 0 },
        zoomOutLimit: 365 * 24 * 60 * 60 * 1000,
        zoomInLimit: 1000,
      },
      {
        timeSystem: 'utc',
        bounds: { start: Date.now() - 15 * 60 * 1000, end: Date.now() },
        zoomOutLimit: 365 * 24 * 60 * 60 * 1000,
        zoomInLimit: 1000,
      },
    ],
  }),
)

openmct.install(AvionicsLayoutPlugin)
openmct.install(TelemetryProviderPlugin)
openmct.install(DataSourceSwitcherPlugin)

openmct.time.setTimeSystem('utc', {
  start: Date.now() - 5 * 60 * 1000,
  end: Date.now(),
})

if (!window.location.hash || window.location.hash === '#/' || window.location.hash === '#/browse/') {
  window.location.hash = '#/browse/caduceus:layout'
}

openmct.start()

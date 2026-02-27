import type { DataSource, Datum } from '../plugins/data-provider'

export const VL_BATTERY_GPS = 'vl_battery_v_gps'
export const VL_BATTERY_RECEIVED = 'vl_battery_v_received'

const TICK_INTERVAL_MS = 10
const GPS_TOGGLE_INTERVAL_MS = 2000

export class FakeDataGenerator implements DataSource {
  /** @returns all keys this generator may emit */
  allKeys(): string[] {
    return [VL_BATTERY_GPS, VL_BATTERY_RECEIVED]
  }

  /**
   * Starts the generator and calls `onData` for each produced datum.
   * Emits VL_BATTERY_GPS when GPS fix is active, VL_BATTERY_RECEIVED otherwise.
   * @param onData callback invoked for every new datum
   */
  subscribe(onData: (data: Datum) => void): void {
    let hasGpsFix = true

    setInterval(() => {
      hasGpsFix = !hasGpsFix
    }, GPS_TOGGLE_INTERVAL_MS)

    setInterval(() => {
      const timestampMs = Date.now()
      const value = Math.sin((2 * Math.PI * timestampMs) / 10000)
      const key = hasGpsFix ? VL_BATTERY_GPS : VL_BATTERY_RECEIVED
      onData({ key, value, timestampMs })
    }, TICK_INTERVAL_MS)
  }
}

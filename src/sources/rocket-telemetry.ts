/**
 * Rocket telemetry catalog and folder tree.
 *
 * Models the browsable `Rocket Telemetry` tree as two top-level directories —
 * Send Uplink and Ground Station Downlink (which itself nests Link Quality and
 * the per-subsystem folders from the downlink dictionary) — each a nested tree
 * of folders and scalar telemetry signals ({@link DataKey}). The
 * {@link RocketTelemetryTreePlugin} turns this structure into OpenMCT folder
 * objects; the {@link DataProviderPlugin} resolves each leaf signal.
 *
 * Signal `key`s are namespaced by directory (`uplink_`, `gsd_`) because the
 * backend stores one scalar series per key.
 */
import type {
  DataKey,
  DataSource,
  Datum,
  Enumeration,
} from '../plugins/data-provider'
import { MODES, RESET_DEVICES } from '../plugins/uplink-command.ts'

// Downlink packets arrive at roughly packet rate, far slower than the 20 ms
// tick of the local fake generator, so use a wider plot gap threshold.
const GAP_MS = 5000

// ---------------------------------------------------------------------------
// Enumerations (§4) — mirror the firmware shared enums.
// ---------------------------------------------------------------------------

/** Generic boolean flag (online, continuity, fire, reset, …). */
export const BOOL_ENUM: Enumeration[] = [
  { value: 0, string: 'FALSE' },
  { value: 1, string: 'TRUE' },
]

/** `FlightStage` (downlink flight `state`). */
export const FLIGHT_STAGE_ENUM: Enumeration[] = [
  { value: 0, string: 'LowPower' },
  { value: 1, string: 'SelfTest' },
  { value: 2, string: 'Armed' },
  { value: 3, string: 'PoweredAscent' },
  { value: 4, string: 'Coasting' },
  { value: 5, string: 'DrogueDeployed' },
  { value: 6, string: 'MainDeployed' },
  { value: 7, string: 'Landed' },
]

/** Received packet type (link-quality `packet_type`). */
export const PACKET_TYPE_ENUM: Enumeration[] = [
  { value: 0, string: 'GPSBeacon' },
  { value: 1, string: 'Telemetry' },
  { value: 2, string: 'LowPowerTelemetry' },
  { value: 3, string: 'LandedTelemetry' },
  { value: 4, string: 'SelfTestResult' },
  { value: 5, string: 'Ack' },
]

/** `PowerOutputOverwrite` (uplink AMP / EPS output overwrites). */
export const POWER_OUTPUT_OVERWRITE_ENUM: Enumeration[] = [
  { value: 0, string: 'No Overwrite' },
  { value: 1, string: 'Enable' },
  { value: 2, string: 'Disable' },
]

/** `PowerOutputStatus` (downlink AMP / EPS output status). */
export const POWER_OUTPUT_STATUS_ENUM: Enumeration[] = [
  { value: 0, string: 'Disabled' },
  { value: 1, string: 'PowerGood' },
  { value: 2, string: 'PowerBad' },
]

/**
 * `NodeHealth` (Self Test node status). PROVISIONAL — only `Healthy` is
 * confirmed from the previous CLI; the remaining variants/order must be
 * reconciled with `firmware-common-new::can_bus::messages::node_status::NodeHealth`
 * (that crate is not vendored here).
 */
export const NODE_HEALTH_ENUM: Enumeration[] = [
  { value: 0, string: 'Healthy' },
  { value: 1, string: 'Degraded' },
  { value: 2, string: 'Unhealthy' },
]

/**
 * `NodeMode` (Self Test node status). PROVISIONAL — only `Operational` is
 * confirmed; reconcile with `firmware-common-new ... NodeMode`.
 */
export const NODE_MODE_ENUM: Enumeration[] = [
  { value: 0, string: 'Operational' },
  { value: 1, string: 'LowPower' },
  { value: 2, string: 'SelfTest' },
]

// ---------------------------------------------------------------------------
// Signal builders
// ---------------------------------------------------------------------------

/**
 * Build a numeric signal descriptor.
 * @param dp decimal places used to render the value (0 → integer-like).
 */
function num(
  key: string,
  name: string,
  unit: string,
  dp: number,
  min: number,
  max: number
): DataKey {
  return {
    key,
    name,
    unit,
    format: `%.${dp}f`,
    min,
    max,
    gapThreshold: GAP_MS,
  }
}

/** Build a boolean (T/F) signal descriptor, rendered as an enum. */
function flag(key: string, name: string): DataKey {
  return {
    key,
    name,
    format: 'enum',
    enumerations: BOOL_ENUM,
    gapThreshold: GAP_MS,
  }
}

/** Build an enum signal descriptor from one of the {@link Enumeration} tables. */
function enm(key: string, name: string, enumerations: Enumeration[]): DataKey {
  return { key, name, format: 'enum', enumerations, gapThreshold: GAP_MS }
}

// ---------------------------------------------------------------------------
// Tree model
// ---------------------------------------------------------------------------

/**
 * A node in the Rocket Telemetry tree: either a `folder` (which contains more
 * nodes) or a `signal` leaf (a single telemetry {@link DataKey}).
 */
export type TreeNode =
  | { kind: 'folder'; id: string; name: string; children: TreeNode[] }
  | { kind: 'signal'; signal: DataKey }

/** Build a folder node. */
function folder(id: string, name: string, children: TreeNode[]): TreeNode {
  return { kind: 'folder', id, name, children }
}

/** Wrap a {@link DataKey} as a signal leaf node. */
function sig(signal: DataKey): TreeNode {
  return { kind: 'signal', signal }
}

// ---------------------------------------------------------------------------
// Directory 1 — Send Uplink (commands modeled as signals, §2).
// ---------------------------------------------------------------------------

/** Build the 3.3V / 5V / 9V overwrite signals for one EPS board. */
function epsOverwriteSignals(n: 1 | 2): TreeNode[] {
  const p = `uplink_overwrite_eps${n}`
  return [
    sig(enm(`${p}_3v3`, '3.3V', POWER_OUTPUT_OVERWRITE_ENUM)),
    sig(enm(`${p}_5v`, '5V', POWER_OUTPUT_OVERWRITE_ENUM)),
    sig(enm(`${p}_9v`, '9V', POWER_OUTPUT_OVERWRITE_ENUM)),
  ]
}

export const SEND_UPLINK = folder('uplink', 'Send Uplink', [
  folder('uplink_config', 'Config', [
    sig(num('uplink_config_frequency', 'Frequency', 'MHz', 3, 902, 928)),
    sig(num('uplink_config_power', 'Power', 'dBm', 0, 0, 30)),
  ]),
  sig(num('uplink_target_apogee', 'Target Apogee', 'm', 0, 0, 10000)),
  ...MODES.map((m) => sig(flag(`uplink_mode_${m.key}`, `${m.label} Mode`))),
  folder(
    'uplink_reset',
    'Reset Device',
    RESET_DEVICES.map((d) => sig(flag(`uplink_reset_${d.key}`, d.label)))
  ),
  folder('uplink_overwrite_amp', 'Overwrite AMP', [
    sig(enm('uplink_overwrite_amp_out1', 'Out 1', POWER_OUTPUT_OVERWRITE_ENUM)),
    sig(enm('uplink_overwrite_amp_out2', 'Out 2', POWER_OUTPUT_OVERWRITE_ENUM)),
    sig(enm('uplink_overwrite_amp_out3', 'Out 3', POWER_OUTPUT_OVERWRITE_ENUM)),
    sig(enm('uplink_overwrite_amp_out4', 'Out 4', POWER_OUTPUT_OVERWRITE_ENUM)),
  ]),
  folder('uplink_overwrite_eps', 'Overwrite EPS', [
    folder('uplink_overwrite_eps1', 'EPS 1', epsOverwriteSignals(1)),
    folder('uplink_overwrite_eps2', 'EPS 2', epsOverwriteSignals(2)),
  ]),
  sig(flag('uplink_fire_main_pyro', 'Fire Main Pyro')),
  sig(flag('uplink_fire_drogue_pyro', 'Fire Drogue Pyro')),
])

// ---------------------------------------------------------------------------
// Directory 2 — Ground Station Downlink.
//
// Mirrors the OpenMCT object tree in `OPENMCT_GROUND_STATION_DOWNLINK_TELEMETRY.md`
// (keys, names, units, and enums from that dictionary). Keys are prefixed `gsd_`
// to stay unique in the OpenMCT identifier namespace; the suffix is the raw Rust
// JSON key.
// ---------------------------------------------------------------------------

/** Build the AMP output status + overwrote signals (outputs 1–4). */
function ampOutputSignals(): TreeNode[] {
  const out: TreeNode[] = []
  for (let i = 1; i <= 4; i++) {
    out.push(
      sig(
        enm(
          `gsd_amp_out${i}`,
          `AMP Output ${i} Status`,
          POWER_OUTPUT_STATUS_ENUM
        )
      ),
      sig(flag(`gsd_amp_out${i}_overwrote`, `AMP Output ${i} Overwrote`))
    )
  }
  return out
}

/** Build one Payload EPS board folder (identical field set for boards 1 and 2). */
function epsBoard(n: 1 | 2): TreeNode {
  const p = `gsd_eps${n}`
  const label = `EPS ${n}`
  const signals: TreeNode[] = [
    sig(flag(`${p}_online`, `${label} Online`)),
    sig(flag(`${p}_rebooted_in_last_5s`, `${label} Rebooted (<5 s)`)),
    sig(num(`${p}_battery1_v`, `${label} Battery 1 Voltage`, 'V', 2, 2.0, 4.5)),
    sig(
      num(
        `${p}_battery1_temperature`,
        `${label} Battery 1 Temp`,
        '°C',
        1,
        10,
        85
      )
    ),
    sig(num(`${p}_battery2_v`, `${label} Battery 2 Voltage`, 'V', 2, 2.0, 4.5)),
    sig(
      num(
        `${p}_battery2_temperature`,
        `${label} Battery 2 Temp`,
        '°C',
        1,
        10,
        85
      )
    ),
  ]
  for (const [rail, railLabel] of [
    ['3v3', '3V3'],
    ['5v', '5V'],
    ['9v', '9V'],
  ] as const) {
    signals.push(
      sig(
        num(
          `${p}_output_${rail}_current`,
          `${label} ${railLabel} Output Current`,
          'mA',
          0,
          0,
          2000
        )
      ),
      sig(
        enm(
          `${p}_output_${rail}_status`,
          `${label} ${railLabel} Output Status`,
          POWER_OUTPUT_STATUS_ENUM
        )
      ),
      sig(
        flag(
          `${p}_output_${rail}_overwrote`,
          `${label} ${railLabel} Output Overwrote`
        )
      )
    )
  }
  return folder(p, label, signals)
}

/**
 * Self Test node-status signals (health + mode) for one node. Mirrors the
 * `format_node_status` field in the previous CLI's Self Test packet. The
 * `rebooted` part of that display reuses the node's existing
 * `gsd_<prefix>_rebooted_in_last_5s` signal.
 */
function nodeSelfTest(prefix: string, label: string): TreeNode[] {
  return [
    sig(
      enm(`gsd_selftest_${prefix}_health`, `${label} Health`, NODE_HEALTH_ENUM)
    ),
    sig(enm(`gsd_selftest_${prefix}_mode`, `${label} Mode`, NODE_MODE_ENUM)),
  ]
}

const GROUND_STATION_DOWNLINK = folder('gsd', 'Ground Station Downlink', [
  // Link Quality — LoRa PacketStatus metrics (now nested under the downlink).
  folder('gsd_link', 'Link Quality', [
    sig(num('gsd_rssi', 'RSSI', 'dBm', 0, -140, 0)),
    sig(num('gsd_snr', 'SNR', 'dB', 0, -20, 20)),
    sig(
      num(
        'gsd_seconds_since_received',
        'Time Since Last Packet',
        's',
        0,
        0,
        120
      )
    ),
    sig(enm('gsd_packet_type', 'Packet Type', PACKET_TYPE_ENUM)),
  ]),
  // GPS & Position
  folder('gsd_gps', 'GPS & Position', [
    sig(
      num('gsd_num_of_fix_satellites', 'Satellites (fix)', 'count', 0, 0, 31)
    ),
    sig(flag('gsd_gps_fixed', 'GPS Fixed')),
    sig(flag('gsd_unix_clock_ready', 'Unix Clock Ready')),
    sig(num('gsd_lat', 'Latitude', 'deg', 5, -90, 90)),
    sig(num('gsd_lon', 'Longitude', 'deg', 5, -180, 180)),
    sig(num('gsd_altitude_asl', 'Altitude ASL', 'm', 1, -100, 7000)),
  ]),
  // Flight Dynamics
  folder('gsd_flight', 'Flight Dynamics', [
    sig(enm('gsd_flight_stage', 'Flight Stage', FLIGHT_STAGE_ENUM)),
    sig(num('gsd_altitude_agl', 'Altitude AGL', 'm', 1, -100, 7000)),
    sig(num('gsd_max_altitude_agl', 'Max Altitude AGL', 'm', 1, -100, 7000)),
    sig(num('gsd_air_speed', 'Air Speed', 'm/s', 1, 0, 400)),
    sig(num('gsd_max_air_speed', 'Max Air Speed', 'm/s', 1, 0, 400)),
    sig(num('gsd_tilt_deg', 'Tilt', 'deg', 1, -90, 90)),
    sig(num('gsd_air_temperature', 'Air Temperature', '°C', 1, -10, 85)),
  ]),
  // Vehicle Power & Pyro
  folder('gsd_power', 'Vehicle Power & Pyro', [
    sig(num('gsd_vl_battery_v', 'VL Battery Voltage', 'V', 2, 2.5, 8.5)),
    sig(
      num('gsd_shared_battery_v', 'Shared Battery Voltage', 'V', 2, 2.5, 8.5)
    ),
    sig(flag('gsd_pyro_short_circuit', 'Pyro Short Circuit')),
    sig(flag('gsd_pyro_main_continuity', 'Main Pyro Continuity')),
    sig(flag('gsd_pyro_main_fire', 'Main Pyro Fire')),
    sig(flag('gsd_pyro_drogue_continuity', 'Drogue Pyro Continuity')),
    sig(flag('gsd_pyro_drogue_fire', 'Drogue Pyro Fire')),
  ]),
  // AMP (Power Distribution)
  folder('gsd_amp', 'AMP (Power Distribution)', [
    sig(flag('gsd_amp_online', 'AMP Online')),
    sig(flag('gsd_amp_rebooted_in_last_5s', 'AMP Rebooted (<5 s)')),
    ...ampOutputSignals(),
  ]),
  // Bulkheads
  folder('gsd_bulkheads', 'Bulkheads', [
    folder('gsd_main_bulkhead', 'Main Bulkhead', [
      sig(flag('gsd_main_bulkhead_online', 'Main Bulkhead Online')),
      sig(
        flag(
          'gsd_main_bulkhead_rebooted_in_last_5s',
          'Main Bulkhead Rebooted (<5 s)'
        )
      ),
      sig(
        num(
          'gsd_main_bulkhead_brightness',
          'Main Bulkhead Brightness',
          'lux',
          2,
          0,
          100000
        )
      ),
    ]),
    folder('gsd_drogue_bulkhead', 'Drogue Bulkhead', [
      sig(flag('gsd_drogue_bulkhead_online', 'Drogue Bulkhead Online')),
      sig(
        flag(
          'gsd_drogue_bulkhead_rebooted_in_last_5s',
          'Drogue Bulkhead Rebooted (<5 s)'
        )
      ),
      sig(
        num(
          'gsd_drogue_bulkhead_brightness',
          'Drogue Bulkhead Brightness',
          'lux',
          2,
          0,
          100000
        )
      ),
    ]),
  ]),
  // ICARUS — Air Brakes
  folder('gsd_icarus', 'ICARUS — Air Brakes', [
    sig(flag('gsd_icarus_online', 'ICARUS Online')),
    sig(flag('gsd_icarus_rebooted_in_last_5s', 'ICARUS Rebooted (<5 s)')),
    sig(
      num(
        'gsd_air_brakes_commanded_extension_percentage',
        'Air Brakes Commanded Extension',
        '%',
        0,
        0,
        100
      )
    ),
    sig(
      num(
        'gsd_air_brakes_actual_extension_percentage',
        'Air Brakes Actual Extension',
        '%',
        0,
        0,
        100
      )
    ),
    sig(
      num(
        'gsd_air_brakes_servo_temp',
        'Air Brakes Servo Temp',
        '°C',
        1,
        -10,
        85
      )
    ),
  ]),
  // OZYS strain/data-acquisition nodes
  folder('gsd_ozys1', 'OZYS 1', [
    sig(flag('gsd_ozys1_online', 'OZYS 1 Online')),
    sig(flag('gsd_ozys1_rebooted_in_last_5s', 'OZYS 1 Rebooted (<5 s)')),
    sig(num('gsd_ozys1_disk_usage', 'OZYS 1 Disk Usage', '%', 0, 0, 100)),
  ]),
  folder('gsd_ozys2', 'OZYS 2', [
    sig(flag('gsd_ozys2_online', 'OZYS 2 Online')),
    sig(flag('gsd_ozys2_rebooted_in_last_5s', 'OZYS 2 Rebooted (<5 s)')),
    sig(num('gsd_ozys2_disk_usage', 'OZYS 2 Disk Usage', '%', 0, 0, 100)),
  ]),
  // Payload EPS
  folder('gsd_eps', 'Payload EPS', [epsBoard(1), epsBoard(2)]),
  // Avionics Status — remaining node health flags
  folder('gsd_avionics', 'Avionics Status', [
    sig(
      flag('gsd_payload_activation_pcb_online', 'Payload Activation PCB Online')
    ),
    sig(
      flag(
        'gsd_payload_activation_pcb_rebooted_in_last_5s',
        'Payload Activation PCB Rebooted (<5 s)'
      )
    ),
    sig(flag('gsd_rocket_wifi_online', 'Rocket WiFi Online')),
    sig(
      flag('gsd_rocket_wifi_rebooted_in_last_5s', 'Rocket WiFi Rebooted (<5 s)')
    ),
  ]),
  // Self Test — fields carried only by the SelfTestResult packet.
  folder('gsd_selftest', 'Self Test', [
    sig(flag('gsd_selftest_imu_ok', 'IMU OK')),
    sig(flag('gsd_selftest_baro_ok', 'Baro OK')),
    sig(flag('gsd_selftest_mag_ok', 'Mag OK')),
    sig(flag('gsd_selftest_gps_ok', 'GPS OK')),
    sig(flag('gsd_selftest_sd_ok', 'SD OK')),
    sig(flag('gsd_selftest_can_bus_ok', 'CAN Bus OK')),
    ...nodeSelfTest('amp', 'AMP'),
    sig(flag('gsd_selftest_amp_out1_power_good', 'AMP Out 1 Good')),
    sig(flag('gsd_selftest_amp_out2_power_good', 'AMP Out 2 Good')),
    sig(flag('gsd_selftest_amp_out3_power_good', 'AMP Out 3 Good')),
    sig(flag('gsd_selftest_amp_out4_power_good', 'AMP Out 4 Good')),
    ...nodeSelfTest('icarus', 'ICARUS'),
    ...nodeSelfTest('ozys1', 'OZYS 1'),
    ...nodeSelfTest('ozys2', 'OZYS 2'),
    ...nodeSelfTest('main_bulkhead', 'Main Bulkhead PCB'),
    ...nodeSelfTest('drogue_bulkhead', 'Drogue Bulkhead PCB'),
    ...nodeSelfTest('payload_activation_pcb', 'Payload Activation PCB'),
    ...nodeSelfTest('rocket_wifi', 'Rocket WiFi'),
    ...nodeSelfTest('eps1', 'Payload EPS 1'),
    ...nodeSelfTest('eps2', 'Payload EPS 2'),
  ]),
])

/** The top-level directories under `Rocket Telemetry`, in display order. */
export const ROCKET_TREE: TreeNode[] = [SEND_UPLINK, GROUND_STATION_DOWNLINK]

/** Flat list of every rocket telemetry signal across the whole tree. */
export const ROCKET_SIGNALS: DataKey[] = (function flatten(
  nodes: TreeNode[]
): DataKey[] {
  const out: DataKey[] = []
  for (const node of nodes) {
    if (node.kind === 'signal') out.push(node.signal)
    else out.push(...flatten(node.children))
  }
  return out
})(ROCKET_TREE)

// ---------------------------------------------------------------------------
// Fake generator (for the DuckDB / local-demo backend)
// ---------------------------------------------------------------------------

const TICK_MS = 500

/**
 * Produce a plausible sample for a signal so the dashboard shows live motion.
 * Numeric signals sweep within their declared [min, max]; enum/boolean signals
 * cycle through their values.
 */
function sampleSignal(signal: DataKey, index: number, now: number): number {
  if (signal.enumerations) {
    if (signal.enumerations === BOOL_ENUM) {
      // Mostly TRUE, dipping FALSE occasionally and out of phase per signal.
      return Math.sin(now / 7000 + index) > -0.6 ? 1 : 0
    }
    const slot = Math.floor(now / 3000 + index) % signal.enumerations.length
    return signal.enumerations[slot].value
  }
  const min = signal.min ?? 0
  const max = signal.max ?? 1
  const frac = 0.5 + 0.45 * Math.sin(now / 8000 + index)
  return min + (max - min) * frac
}

/**
 * Emits fake values for every {@link ROCKET_SIGNALS} entry, so the OpenMCT
 * dashboard is populated without a live ground-station link.
 */
export class RocketTelemetryGenerator implements DataSource {
  /** @returns all rocket telemetry key descriptors. */
  static allKeys(): DataKey[] {
    return ROCKET_SIGNALS
  }

  allKeys(): DataKey[] {
    return ROCKET_SIGNALS
  }

  /**
   * Starts the generator, emitting one datum per signal on every tick.
   * @param onData callback invoked for every produced datum
   */
  subscribe(onData: (data: Datum) => void): void {
    setInterval(() => {
      const now = Date.now()
      for (let i = 0; i < ROCKET_SIGNALS.length; i++) {
        const signal = ROCKET_SIGNALS[i]
        onData({
          key: signal.key,
          value: sampleSignal(signal, i, now),
          timestampMs: now,
        })
      }
    }, TICK_MS)
  }
}



/**
 * Receives triples sent from the backend server for each {@link ROCKET_SIGNALS} entry,
 * so the OpenMCT dashboard can be populated from either real data or the backend's mock data.
 */
export class GroundStationSource implements DataSource {
  /** @returns all rocket telemetry key descriptors. */
  static allKeys(): DataKey[] {
    return ROCKET_SIGNALS
  }

  allKeys(): DataKey[] {
    return ROCKET_SIGNALS
  }
  subscribe(onData: (data: Datum) => void): void {
    this.connect(onData);
  }
  connect(onData: (data: Datum) => void): void {
    const socket = new WebSocket("ws://localhost:3000/ws");

    socket.addEventListener("open", (event: Event) => {
      console.log("Connected to the WebSocket server.");
    });

    // Event: Listen for incoming messages
    socket.addEventListener("message", (event: MessageEvent) => {
      console.log("Message from server:", event.data);
      
      // if json switch, const data = JSON.parse(event.data);
      try {
        // Data should be in form (key, value, timestamp)
        const triple = event.data.split(",");
        console.log(triple)
        console.log(triple[0].slice(1), Number(triple[1]), Number(triple[2].slice(0, -1)))
        onData({
          key: triple[0].slice(1), // remove leading bracket
          value: Number(triple[1]),
          timestampMs: Number(triple[2].slice(0, -1)), // remove trailing bracket and convert to number
        });
      } catch (err) {
        // Handle plain text or other formats
        console.log("Error receiving triple")
        console.log(err)
      }
    });

    // Event: Connection closed
    socket.addEventListener("close", (event: CloseEvent) => {
      console.log(`Disconnected. Code: ${event.code}, Reason: ${event.reason}`);
      setTimeout(() => {
        this.connect(onData);
      }, 1000);
    });

    // Event: Handle errors
    socket.addEventListener("error", (event: Event) => {
      console.error("WebSocket error observed:", event);
    });
  }
}

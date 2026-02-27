export interface TelemetryDatum {
  timestampMs: number
  value: number
}

export interface QueryOptions {
  strategy?: 'minmax' | 'latest'
  size?: number
}

export interface TelemetryBackend {
  init(): Promise<void>
  /**
   * @param key          datum key (used as table/measurement name)
   * @param timestampMs  timestamp in ms
   * @param value        numeric value
   */
  insertTelemetry(
    key: string,
    timestampMs: number,
    value: number
  ): Promise<void>
  /**
   * @param key    datum key
   * @param start  start of time range (timestampMs), inclusive
   * @param end    end of time range (timestampMs), inclusive
   */
  queryTelemetry(
    key: string,
    start: number,
    end: number,
    options?: QueryOptions
  ): Promise<TelemetryDatum[]>
}

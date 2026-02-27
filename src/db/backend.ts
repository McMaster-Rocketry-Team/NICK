export interface TelemetryDatum {
  timestamp: number | null
  receivedTimestamp: number
  value: number
}

export type TelemetrySeries = 'gps' | 'received'

export interface QueryOptions {
  strategy?: 'minmax' | 'latest'
  size?: number
}

export interface TelemetryBackend {
  init(): Promise<void>
  insertTelemetry(
    table: string,
    timestamp: number | null,
    receivedTimestamp: number,
    value: number,
  ): Promise<void>
  queryTelemetry(
    table: string,
    series: TelemetrySeries,
    start: number,
    end: number,
    options?: QueryOptions,
  ): Promise<TelemetryDatum[]>
}

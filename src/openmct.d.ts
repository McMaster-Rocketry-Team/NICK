declare module '@mcmaster-rocketry-team/openmct' {
  const openmct: any
  export default openmct
}

declare module 'virtual:pwa-register' {
  export function registerSW(options?: {
    immediate?: boolean
    onNeedRefresh?: () => void
    onOfflineReady?: () => void
    onRegistered?: (registration: ServiceWorkerRegistration | undefined) => void
    onRegisterError?: (error: unknown) => void
  }): (reloadPage?: boolean) => Promise<void>
}

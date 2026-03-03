import path from 'path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { VitePWA } from 'vite-plugin-pwa'
import tailwindcss from '@tailwindcss/vite'

const openmctDist = path.resolve(__dirname, 'vendor/openmct/dist')

const base = process.env.BASE_PATH ?? '/'

export default defineConfig({
  base,
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'credentialless',
    },
    fs: {
      allow: ['.', openmctDist],
    },
  },
  build: {
    chunkSizeWarningLimit: 100000,
  },
  plugins: [
    tailwindcss(),
    react(),
    VitePWA({
      registerType: 'autoUpdate',
      includeAssets: ['favicon.ico', 'pwa-192x192.png', 'pwa-512x512.png'],
      manifest: {
        name: 'NICK',
        short_name: 'NICK',
        description: 'Next-gen Instrumentation & Control Kit',
        theme_color: '#fcfcfc',
        background_color: '#fcfcfc',
        display: 'standalone',
        orientation: 'landscape',
        scope: base,
        start_url: base,
        icons: [
          {
            src: 'pwa-192x192.png',
            sizes: '192x192',
            type: 'image/png',
          },
          {
            src: 'pwa-512x512.png',
            sizes: '512x512',
            type: 'image/png',
            purpose: 'any maskable',
          },
        ],
      },
      workbox: {
        // Cache all assets for offline use
        globPatterns: ['**/*'],
        // Large files like wasm need a higher size limit
        maximumFileSizeToCacheInBytes: 100 * 1024 * 1024,
        runtimeCaching: [
          {
            // Cache openmct and app assets
            urlPattern: /^https?:\/\/.+/,
            handler: 'NetworkFirst',
            options: {
              cacheName: 'external-resources',
              expiration: {
                maxEntries: 50,
                maxAgeSeconds: 7 * 24 * 60 * 60,
              },
            },
          },
        ],
      },
    }),
  ],
})

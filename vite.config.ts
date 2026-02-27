import { defineConfig } from 'vite'
import path from 'path'

const openmctDist = path.resolve(__dirname, 'node_modules/openmct/dist')

export default defineConfig({
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'credentialless',
    },
    fs: {
      allow: ['.', openmctDist],
    },
  },
})

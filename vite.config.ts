import { defineConfig } from 'vite'
import path from 'path'

const openmctDist = path.resolve(__dirname, 'node_modules/openmct/dist')

export default defineConfig({
  server: {
    fs: {
      allow: ['.', openmctDist],
    },
  },
})

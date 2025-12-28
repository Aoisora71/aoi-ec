import { fileURLToPath } from 'url'
import { dirname, resolve } from 'path'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

/** @type {import('next').NextConfig} */
const nextConfig = {
  outputFileTracingRoot: resolve(__dirname),
  typescript: {
    ignoreBuildErrors: true,
  },
  images: {
    unoptimized: true,
  },
  // Allow cross-origin requests from the server IP
  allowedDevOrigins: [
    'http://162.43.44.223',
    'http://162.43.44.223:8000',
    'https://162.43.44.223',
    'https://162.43.44.223:8000',
  ],
  async rewrites() {
    return [
      // Proxy health endpoints to the backend during dev/when hitting Next directly
      {
        source: '/health/full',
        destination: 'http://localhost:8000/health/full',
      },
      {
        source: '/health',
        destination: 'http://localhost:8000/health',
      },
      // Proxy API routes to the backend if they accidentally hit Next.js
      {
        source: '/api/:path*',
        destination: 'http://localhost:8000/api/:path*',
      },
    ]
  },
}

export default nextConfig

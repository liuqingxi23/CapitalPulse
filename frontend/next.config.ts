import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  output: "standalone",
  reactStrictMode: true,
  // Proxy /api/finance/* requests to backend.
  // VANE_API_URL is a server-only variable (runtime-configurable in Docker).
  // Defaults to localhost:8000 for local development.
  async rewrites() {
    const apiUrl = process.env.VANE_API_URL ?? "http://localhost:8000";
    return [
      {
        source: "/api/finance/:path*",
        destination: `${apiUrl}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;

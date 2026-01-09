module.exports = {
  apps: [
    {
      name: 'aoi-ec-client',
      cwd: './client',
      script: 'pnpm',
      args: 'start',
      interpreter: 'none',
      env: {
        NODE_ENV: 'production',
        PORT: 6009
      },
      error_file: './logs/client-error.log',
      out_file: './logs/client-out.log',
      log_file: './logs/client-combined.log',
      time: true,
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G'
    },
    {
      name: 'aoi-ec-server',
      cwd: './server',
      script: './venv/bin/python',
      args: 'api_server.py',
      interpreter: 'none',
      env: {
        API_PORT: 8000,
        PYTHONUNBUFFERED: '1'
      },
      error_file: './logs/server-error.log',
      out_file: './logs/server-out.log',
      log_file: './logs/server-combined.log',
      time: true,
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G'
    }
  ]
};

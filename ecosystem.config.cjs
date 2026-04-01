module.exports = {
  apps: [{
    name: 'roco',
    script: 'index.js',
    interpreter: 'node',
    interpreter_args: '--experimental-vm-modules',
    cwd: '/root/roco',

    // Restart policy
    autorestart: true,          // always restart on crash
    max_restarts: 50,           // allow up to 50 restarts
    min_uptime: '10s',          // must stay up 10s to count as stable
    restart_delay: 2000,        // wait 2s between restarts

    // Memory limit — restart if Roco uses more than 512MB
    max_memory_restart: '512M',

    // Environment
    env_file: '/root/roco/.env',
    env: {
      NODE_ENV: 'production',
      PORT: 3000,
    },

    // Logging
    out_file: '/root/roco/logs/roco-out.log',
    error_file: '/root/roco/logs/roco-error.log',
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
    merge_logs: true,

    // Watch — OFF in production (causes restart loops)
    watch: false,

    // Kill timeout — give 10s for graceful shutdown
    kill_timeout: 10000,

    // Node.js flags
    node_args: '--max-old-space-size=400',
  }]
};

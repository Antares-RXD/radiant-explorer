module.exports = {
  apps: [
    {
      name: 'neurai-explorer-frontend',
      script: 'server.js',
      instances: '4', // Number of CPU cores to utilize
      exec_mode: 'cluster',
      env: {
        NODE_ENV: 'production',
        PORT: 3000,
      },
    },
  ],
};

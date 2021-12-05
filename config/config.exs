use Mix.Config
require Logger

config :epgproxy,
  db_host: "127.0.0.1",
  db_port: 5432,
  db_name: "postgres",
  db_user: "postgres",
  db_password: "postgres",
  pool_size: 10

# Configures Elixir's Logger
config :logger, :console,
  format: "$time [$level] $message $metadata\n\n",
  metadata: [:error_code, :mfa, :pid]

# config :logger, level: :warning
config :logger, level: :debug

import_config "#{Mix.env()}.exs"

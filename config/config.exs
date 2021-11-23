use Mix.Config
require Logger

config :epgproxy,
  pool_size: 10

# Configures Elixir's Logger
config :logger, :console,
  format: "$time [$level] $message $metadata\n\n",
  metadata: [:error_code, :mfa, :pid]

# config :logger, level: :warning
config :logger, level: :debug

import_config "#{Mix.env()}.exs"

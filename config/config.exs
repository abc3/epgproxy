use Mix.Config
require Logger

# Configures Elixir's Logger
config :logger, :console,
  format: "$time [$level] $message $metadata\n\n",
  metadata: [:error_code, :mfa, :pid]

import_config "#{Mix.env()}.exs"

use Mix.Config
require Logger

config :epgproxy,
  connect_timeout: 5000,
  application_name: "epgproxy",
  pool_size: 1

config :epgproxy, EpgproxyWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "avkADKHIgKPHUDT/v2nsXvtphuVGHVKNZp1dD3f2/9unPHIVDYu/jqDjSrCBQDR0",
  render_errors: [view: EpgproxyWeb.ErrorView, accepts: ~w(html json), layout: false],
  pubsub_server: Epgproxy.PubSub,
  live_view: [signing_salt: "6Y94/rs7"]

config :logger, :console,
  format: "$time [$level] $message $metadata\n\n",
  metadata: [:error_code, :mfa, :pid]

# config :logger, level: :warning
config :logger, level: :debug

config :phoenix, :json_library, Jason

import_config "#{Mix.env()}.exs"

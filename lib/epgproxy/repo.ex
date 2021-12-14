defmodule Epgproxy.Repo do
  use Ecto.Repo,
    otp_app: :epgproxy,
    adapter: Ecto.Adapters.Postgres
end

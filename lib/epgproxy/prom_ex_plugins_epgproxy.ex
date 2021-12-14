defmodule Epgproxy.PromEx.Plugins.Epgproxy do
  use PromEx.Plugin
  require Logger
  alias TelemetryMetricsPrometheus.Core

  @db_sess_size [:prom_ex, :plugin, :epgproxy, :db_sess_size]
  @client_sess_size [:prom_ex, :plugin, :epgproxy, :client_sess_size]

  @impl true
  def polling_metrics(opts) do
    poll_rate = Keyword.get(opts, :poll_rate, 5_000)

    [
      channel_metrics(poll_rate)
    ]
  end

  defp channel_metrics(poll_rate) do
    Polling.build(
      :epgproxy_polling_events,
      poll_rate,
      {__MODULE__, :execute_metrics, []},
      [
        last_value(
          [:epgproxy, :db_sess_size],
          event_name: @db_sess_size,
          description: "Connections to DB",
          measurement: :db_sess_size
        ),
        last_value(
          [:epgproxy, :client_sess_size],
          event_name: @client_sess_size,
          description: "Clients connected to epgproxy",
          measurement: :client_sess_size
        )
      ]
    )
  end

  def execute_metrics() do
    :telemetry.execute(@db_sess_size, %{db_sess_size: db_sess_size()}, %{})
    :telemetry.execute(@client_sess_size, %{client_sess_size: client_sess_size()}, %{})
  end

  def db_sess_size() do
    Registry.count_match(Registry.EpgproxyStats, "db_sess_size", :_)
  end

  def client_sess_size() do
    Registry.count_match(Registry.EpgproxyStats, "client_sess_size", :_)
  end
end

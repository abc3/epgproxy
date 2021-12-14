defmodule Epgproxy.Application do
  use Application

  @impl true
  def start(_type, _args) do
    {:ok, _} = Registry.start_link(keys: :duplicate, name: Registry.EpgproxyStats)

    :ranch.start_listener(
      :pg_proxy,
      :ranch_tcp,
      # :ranch_ssl,
      %{socket_opts: [{:port, 5555}]},
      Epgproxy.ClientSess,
      []
    )

    children = [
      :poolboy.child_spec(:worker, poolboy_config()),
      Epgproxy.PromEx,
      EpgproxyWeb.Telemetry,
      {Phoenix.PubSub, name: Epgproxy.PubSub},
      EpgproxyWeb.Endpoint
    ]

    opts = [strategy: :one_for_one, name: Epgproxy.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp poolboy_config do
    [
      name: {:local, :db_sess},
      worker_module: Epgproxy.DbSess,
      size: Application.get_env(:epgproxy, :pool_size),
      max_overflow: 0
    ]
  end

  @impl true
  def config_change(changed, _new, removed) do
    EpgproxyWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end

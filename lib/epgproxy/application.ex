defmodule Epgproxy.Application do
  use Application

  @impl true
  def start(_type, _args) do
    :ranch.start_listener(
      :pg_proxy,
      :ranch_tcp,
      # :ranch_ssl,
      %{socket_opts: [{:port, 5555}]},
      Epgproxy.ClientSess,
      []
    )

    children = [
      # %{
      #   id: Epgproxy.DbSess,
      #   start: {Epgproxy.DbSess, :start_link, [nil]}
      # }
      :poolboy.child_spec(:worker, poolboy_config())
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
end

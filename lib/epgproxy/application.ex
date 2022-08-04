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

    def = "postgres://postgres:postgres@localhost:5432/postgres"
    primary_creds = System.get_env("PRIMARY_DB", def) |> uri_to_config()
    secondary_creds = System.get_env("SECONDARY_DB", def) |> uri_to_config()

    children = [
      :poolboy.child_spec(:primary, poolboy_config(:primary), type: :primary, creds: primary_creds),
      :poolboy.child_spec(:secondary, poolboy_config(:secondary),
        type: :secondary,
        creds: secondary_creds
      ),
      Epgproxy.PromEx,
      EpgproxyWeb.Telemetry,
      {Phoenix.PubSub, name: Epgproxy.PubSub},
      EpgproxyWeb.Endpoint
    ]

    opts = [strategy: :one_for_one, name: Epgproxy.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def poolboy_config(id) do
    [
      name: {:local, id},
      worker_module: Epgproxy.DbSess,
      size: Application.get_env(:epgproxy, :pool_size),
      max_overflow: 0
    ]
  end

  def uri_to_config(uri) do
    parsed = URI.parse(uri)
    [user, password] = String.split(parsed.userinfo, ":")
    "/" <> name = parsed.path

    %{
      host: parsed.host,
      port: parsed.port,
      name: name,
      user: user,
      password: password
    }
  end

  @impl true
  def config_change(changed, _new, removed) do
    EpgproxyWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end

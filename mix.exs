defmodule Epgproxy.MixProject do
  use Mix.Project

  def project do
    [
      app: :epgproxy,
      version: "0.1.0",
      elixir: "~> 1.7",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: [:phoenix, :gettext] ++ Mix.compilers(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Epgproxy.Application, []},
      extra_applications: [:logger, :runtime_tools, :ranch, :prom_ex]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:phoenix, "~> 1.5.1"},
      {:phoenix_ecto, "~> 4.1"},
      {:ecto_sql, "~> 3.4"},
      {:postgrex, "~> 0.16.5"},
      {:phoenix_html, "~> 2.11"},
      {:phoenix_live_reload, "~> 1.2", only: :dev},
      {:phoenix_live_dashboard, "~> 0.2.0"},
      {:telemetry_metrics, "~> 0.4"},
      {:telemetry_poller, "~> 0.4"},
      {:gettext, "~> 0.11"},
      {:plug_cowboy, "~> 2.0"},
      {:prom_ex, "~> 1.4.1"},
      {:poolboy, "~> 1.5.2"},
      {:pgo, "~> 0.12"},
      {:epg_query, "~> 0.1.0", git: "https://github.com/abc3/epg_query"}
    ]
  end
end

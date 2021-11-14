defmodule Epgproxy.MixProject do
  use Mix.Project

  def project do
    [
      app: :epgproxy,
      version: "0.1.0",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Epgproxy.Application, []}
    ]
  end

  defp deps do
    [
      {:ranch, "~> 2.1.0"},
      {:pgo, "~> 0.11"}
    ]
  end
end

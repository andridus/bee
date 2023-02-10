defmodule Bee.MixProject do
  use Mix.Project

  def project do
    [
      app: :bee,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Bee.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.6"},
      {:jason, "~> 1.2"},
      {:value, github: "team-softaliza/value"},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ecto_sqlite3, "~> 0.8.2", only: [:test]},
    ]
  end
end

defmodule Bee.MixProject do
  use Mix.Project

  def project do
    [
      name: "bee",
      source_url: "https://github.com/andridus/bee",
      app: :bee,
      version: "0.2.2",
      elixir: "~> 1.12",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
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

  defp description() do
    "Api tools for entity on Ecto"
  end

  defp package() do
    [
      # This option is only needed when you don't want to use the OTP application name
      name: "bee",
      # These are the default files included in the package
      files: ~w(lib .formatter.exs mix.exs README.md),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/andridus/bee"}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.6"},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
      {:ecto_sqlite3, "~> 0.8.2", only: [:test]},
      {:ex_doc, "~> 0.14", only: :dev, runtime: false}
    ]
  end
end

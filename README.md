# Bee

Documentation for [Bee](https://hexdocs.pm/bee/Bee.html).

Bee generate an Api for given Ecto Schema.

For example, you could specify a `User` entity as follows:

```elixir
  defmodule User do

    use Ecto.Schema
    use Bee.Schema

    generate_bee do
      schema "users" do
        field :name, :string
        field :password, :string
        field :permission, Ecto.Enum, values: [:basic, :manager, :admin], default: :basic
        timestamps()
      end
    end

    defmodule User.Api do
      @schema User
      use Bee.Api
    end

  end

  
  User.Api.all(where: [permission: :basic])
```

## Installation

The package can be installed by adding `bee` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:bee, "~> 0.2.0"}
  ]
end
```
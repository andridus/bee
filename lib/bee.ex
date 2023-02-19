defmodule Bee do
  @moduledoc """
  Documentation for `Bee`.

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

        defmodule Api do
          @schema User
          use Bee.Api
        end
      end

      User.Api.all(where: [permission: :basic])
    ```
  """

  @doc """
  Return unique token from length.

  ### Options
    * `:only_numbers` - boolean, forces to return only numbers (integer)
    * `:with_zeros` - boolean, forces to return only numbers, but zero in first position is possible, return string

  ## Example
      "ai0ruwr9pc" = Bee.unique(10)
      "0647250296" = Bee.unique(10, only_numbers: true, with_zeros: true)
       4796925652  = Bee.unique(10, only_numbers: true)
  """
  @type option :: {:only_numbers, boolean()} | {:with_zeros, boolean()}
  @spec unique(len :: integer(), opts :: [option]) :: String.t | integer()
  def unique(len, opts \\ []) do
    is_only_numbers = opts[:only_numbers] || false
    with_zeros = opts[:with_zeros] || false

    cond do
      is_only_numbers && with_zeros && len > 0 ->
        gen_rnd(len, "1234567890")

      is_only_numbers && len > 0 ->
        (gen_rnd(1, "123456789") <> gen_rnd(len - 1, "1234567890")) |> String.to_integer()

      len > 0 ->
        gen_rnd(len, "abcdefghijklmnopqrstuvwxyz1234567890")

      :else ->
        raise "not supported length '#{len}'"
    end
  end

  defp gen_rnd(0, _symbols), do: nil
  defp gen_rnd(1, symbols), do: String.at(symbols, :rand.uniform(String.length(symbols) - 1))

  defp gen_rnd(to, symbols),
    do:
      Enum.map_join(1..to, fn _ ->
        String.at(symbols, :rand.uniform(String.length(symbols)) - 1)
      end)
end

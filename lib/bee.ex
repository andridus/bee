defmodule Bee do
  @moduledoc """
  Documentation for `Bee`.
  """

  @doc """
    Return unique token from length.

    # alphanumeric
    iex> result = Bee.unique(10)
    iex> String.length(result)
    10

     # only numbers with zeros
    iex> result = Bee.unique(10, only_numbers: true, with_zeros: true)
    iex> String.length(result)
    10

    # only numbers
    iex> result = Bee.unique(10, only_numbers: true)
    iex> is_integer(result)
    true
  """
  def unique(len, opts \\ []) do
    is_only_numbers = opts[:only_numbers] || false
    with_zeros = opts[:with_zeros] || false
    cond do
      is_only_numbers && with_zeros && len > 0 -> gen_rnd(len, "1234567890")
      is_only_numbers && len > 0 -> (gen_rnd(1, "123456789") <> gen_rnd(len-1, "1234567890")) |> String.to_integer()
      len > 0 -> gen_rnd(len,  "abcdefghijklmnopqrstuvwxyz1234567890")
      :else -> raise "not supported length '#{len}'"
    end
  end

  defp gen_rnd(0, _symbols), do: nil
  defp gen_rnd(1,  symbols), do: String.at(symbols, :rand.uniform(String.length(symbols)))
  defp gen_rnd(to, symbols), do: for _ <- 1..to, into: "", do: String.at(symbols, :rand.uniform(String.length(symbols)) - 1 )
end

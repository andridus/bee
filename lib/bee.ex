defmodule Bee do
  @moduledoc """
  Documentation for `Bee`.
  """
  def unique(len) do
    gen_rnd(len, "abcdefghijklmnopqrstuvwxyz1234567890")
  end

  defp gen_rnd(to, al) do
    # DateTime.utc_now |> DateTime.to_unix(:millisecond)
    len = String.length(al)
    x = fn _x -> String.at(al, :rand.uniform(len)) end
    1..to |> Enum.map_join(x)
  end
end

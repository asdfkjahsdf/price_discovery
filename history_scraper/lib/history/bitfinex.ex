defmodule History.Bitfinex do
  @moduledoc """
  http://docs.bitfinex.com/v2/reference (trades)

    [
      [
        ID,     <= int id of transaction
        MTS,    <= int millisecond time stamp
        AMOUNT, <= float How much was bought (positive) or sold (negative).
        PRICE   <= float Price at which the trade was executed
      ]
    ]
  """
  use GenServer
  require Logger

  @period 2_000 # two seconds
  @url "https://api.bitfinex.com/v2/trades/tBTCUSD/hist"

  def start_link(last) do
    GenServer.start_link(__MODULE__, last, name: __MODULE__)
  end


  def init(last) do
    {:ok, req(last)}
  end

  def handle_info(:tick, last) do
    {:noreply, req(last)}
  end


  defp req(last) do
    resp = HTTPoison.get!("#{@url}?limit=1000&end=#{last - 1}").body |> Poison.decode!
    [_, new_last, _, _] = List.last(resp)
    write_csv(resp, last, new_last)
    Process.send_after(self(), :tick, @period)
    new_last
  end

  defp write_csv(resp, old_last, new_last) do
    Logger.debug("writing #{old_last}:#{new_last}")

    resp
    |> Stream.map(&Enum.join(&1, ","))
    |> Enum.join("\n")
    |> History.CSV.write("bitfinex2", "#{old_last}:#{new_last}")
  end
end

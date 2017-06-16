defmodule History.BitMEX do
  use GenServer
  require Logger

  @period 2_000
  @url "https://www.bitmex.com/api/v1/trade?symbol=XBTUSD&count=500&reverse=false"

  def start_link(last) do
    GenServer.start_link(__MODULE__, last, name: __MODULE__)
  end


  def init(last) do
    Process.send_after(self(), :tick, @period)
    {:ok, last}
  end

  def handle_info(:tick, last) do
    Process.send_after(self(), :tick, @period)
    {:noreply, req(last)}
  end


  defp req(old_last) do
    resp = HTTPoison.get!("#{@url}&startTime=#{old_last}").body |> Poison.decode!
    %{"timestamp" => new_last} = List.last(resp)
    write_csv(resp, old_last, new_last)
    new_last
  end

  defp write_csv(resp, old_last, new_last) do
    Logger.debug("writing #{old_last}:#{new_last}")

    resp
    |> Stream.map(&to_row/1)
    |> Enum.join("\n")
    |> History.CSV.write("bitmex", "#{old_last}:#{new_last}")
  end

  defp to_row(%{"timestamp" => timestamp, "symbol" => symbol, "side" => side,
                "size" => size, "price" => price, "tickDirection" => tick_direction,
                "trdMatchID" => trd_match_id, "grossValue" => gross_value,
                "homeNotional" => home_notional, "foreignNotional" => foreign_notional}) do
    "#{timestamp},#{symbol},#{side},#{size},#{price},#{tick_direction},#{trd_match_id}," <>
    "#{gross_value},#{home_notional},#{foreign_notional}"
  end
end

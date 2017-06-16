defmodule History.GDAX do
  use GenServer
  require Logger

  @period 360
  @url "https://api.gdax.com/products/BTC-USD/trades?limit=100"

  def start_link do
    trade_id = File.read!("priv/dump/gdax/last_trade") |> :erlang.binary_to_term
    GenServer.start_link(__MODULE__, trade_id, name: __MODULE__)
  end

  def start_link(trade_id) do
    GenServer.start_link(__MODULE__, trade_id, name: __MODULE__)
  end


  def init(trade_id) do
    {:ok, req(trade_id)}
  end

  def handle_info(:tick, last) do
    {:noreply, req(last)}
  end


  defp req(last) do
    resp = HTTPoison.get!("#{@url}&after=#{last + 101}").body |> Poison.decode!
    %{"trade_id" => new_last} = List.first(resp)
    %{"trade_id" => start} = List.last(resp)
    write_dump("gdax", new_last)
    write_csv(resp, start, new_last)
    Process.send_after(self(), :tick, @period)
    new_last
  end

  defp write_csv(resp, old_last, new_last) do
    Logger.debug("writing #{old_last}:#{new_last}")

    resp
    |> Stream.map(&to_row/1)
    |> Enum.join("\n")
    |> History.CSV.write("gdax", "#{old_last}:#{new_last}")
  end

  defp write_dump(venue, data) do
    File.write("priv/dump/#{venue}/last_trade", :erlang.term_to_binary(data))
  end

  defp to_row(%{"time" => time, "trade_id" => trade_id, "price" => price,
                "size" => size, "side" => side}) do
    "#{time},#{trade_id},#{price},#{size},#{side}"
  end
end

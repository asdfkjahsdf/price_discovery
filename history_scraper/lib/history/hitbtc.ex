defmodule History.HitBTC do
  use GenServer
  require Logger

  @period 2_000 # two seconds
  @url "https://api.hitbtc.com/api/1/public/BTCUSD/trades?max_results=1000&format_item=object&side=true"

  def start_link do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end


  def init(_) do
    # create a file
    # new_end_timestamp = case check_existing_files() do
    #   nil -> initial_request()
    #   [_, oldest_trade_ts] -> oldest_trade_ts
    # end
    latest_tid = initial_request()

    Process.send_after(self(), :tick, @period)
    {:ok, latest_tid}
  end

  def handle_info(:tick, latest_tid) do
    resp = HTTPoison.get!("#{@url}&from=#{latest_tid}").body |> Poison.decode!
    trades = resp["trades"]

    %{"tid" => new_latest_tid} = List.last(trades)

    write_csv(trades, latest_tid, new_latest_tid)

    Process.send_after(self(), :tick, @period)
    {:noreply, new_latest_tid}
  end


  defp initial_request do
    resp = HTTPoison.get!(@url).body |> Poison.decode!
    trades = resp["trades"]

    %{"tid" => new_latest_tid} = List.last(trades)

    write_csv(trades, 0, new_latest_tid)

    new_latest_tid
  end

  defp check_existing_files do
    case File.ls("priv/hitbtc") do
      {:ok, []} -> nil
      {:ok, [file | _]} -> String.split(file, ":")
    end
  end

  defp write_csv(trades, latest_tid, new_latest_tid) do
    Logger.debug("writing #{latest_tid}:#{new_latest_tid}")
    trades
    |> Stream.map(&to_row/1)
    |> Enum.join("\n")
    |> History.CSV.write("hitbtc", "#{latest_tid}:#{new_latest_tid}")
  end

  defp to_row(%{"date" => date, "price" => price, "amount" => amount, "tid" => tid, "side" => side}) do
    "#{date},#{price},#{amount},#{tid},#{side}"
  end
end

defmodule History.Gemini do
  use GenServer
  require Logger

  # https://api.gemini.com/v1/trades/btcusd?since=1444777457&limit_trades=500
  @period 2_000
  @url "https://api.gemini.com/v1/trades/btcusd?limit_trades=500"

  def start_link do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end


  def init(_) do
    # create a file
    # new_end_timestamp = case check_existing_files() do
    #   nil -> initial_request()
    #   [_, oldest_trade_ts] -> oldest_trade_ts
    # end
    latest_ts = initial_request()

    Process.send_after(self(), :tick, @period)
    {:ok, latest_ts}
  end

  def handle_info(:tick, latest_ts) do
    resp = HTTPoison.get!("#{@url}&timestamp=#{latest_ts}").body |> Poison.decode!

    %{"timestampms" => new_latest_ts} = List.first(resp)

    write_csv(resp, latest_ts, new_latest_ts)

    Process.send_after(self(), :tick, @period)
    {:noreply, new_latest_ts}
  end

  defp initial_request do
    resp = HTTPoison.get!("#{@url}&timestamp=0").body |> Poison.decode!

    %{"timestampms" => start_ts} = List.last(resp)
    %{"timestampms" => new_latest_ts} = List.first(resp)

    write_csv(resp, start_ts, new_latest_ts)

    new_latest_ts
  end

  defp write_csv(resp, start_ts, end_ts) do
    Logger.debug("writing #{start_ts}:#{end_ts}")

    resp
    |> Stream.map(&to_row/1)
    |> Enum.join("\n")
    |> History.CSV.write("gemini", "#{start_ts}:#{end_ts}")
  end

  defp to_row(%{"timestamp" => timestamp, "timestampms" => timestampms, "tid" => tid,
              "price" => price, "amount" => amount, "exchange" => exchange, "type" => type}) do
    "#{timestamp},#{timestampms},#{tid},#{price},#{amount},#{exchange},#{type}"
  end
end

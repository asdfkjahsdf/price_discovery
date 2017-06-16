defmodule History.Poloniex do
  # http://coinmarketcap.com/assets/tether/ ???

  use GenServer
  require Logger

  @period 3_000 # five seconds
  @url "https://poloniex.com/public?command=returnTradeHistory&currencyPair=USDT_BTC"

  def start_link(last \\ 1471170291) do
    GenServer.start_link(__MODULE__, last, name: __MODULE__)
  end


  def init(last) do
    {:ok, req(last)}
  end

  def handle_info(:tick, last) do
    {:noreply, req(last)}
  end


  defp req(last) do
    Logger.debug("query start: #{unix_to_iso(last)} --- end: #{unix_to_iso(get_end(last))}")
    url = "#{@url}&start=#{last}&end=#{get_end(last)}" |> IO.inspect
    resp = HTTPoison.get!(url).body |> Poison.decode!
    %{"date" => first_date} = List.first(resp)
    %{"date" => last_date} = List.last(resp)

    IO.inspect "first_date: #{first_date}"
    IO.inspect "last_date: #{last_date}"

    new_last = iso_to_unix(first_date) |> IO.inspect()

    write_csv(resp, first_date, last_date)
    Process.send_after(self(), :tick, @period)
    new_last
  end

  defp get_end(last) do
    last + 500_000
  end

  defp iso_to_unix(date) do
    {:ok, dt, _} = DateTime.from_iso8601(date <> "Z")
    DateTime.to_unix(dt)
  end

  defp unix_to_iso(date) do
    {:ok, dt} = DateTime.from_unix(date)
    DateTime.to_iso8601(dt)
  end

  defp write_csv(resp, end_timestamp, new_end_timestamp) do
    Logger.debug("writing #{end_timestamp} --- #{new_end_timestamp}")

    resp
    |> Stream.map(&to_row/1)
    |> Enum.join("\n")
    |> History.CSV.write("poloniex2", "#{end_timestamp}:#{new_end_timestamp}")
  end

  defp to_row(%{"globalTradeID" => global_trade_id, "tradeID" => tid, "date" => date,
                "type" => type, "rate" => rate, "amount" => amount, "total" => total}) do
    "#{global_trade_id},#{tid},#{date},#{type},#{rate},#{amount},#{total}"
  end
end

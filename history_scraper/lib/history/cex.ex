defmodule History.CEX do
  use GenServer
  require Logger

  @period 2_000 # two seconds
  @url "https://cex.io/api/trade_history/BTC/USD/"

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
    resp = [%{"tid" => tid} | _] = HTTPoison.get!("#{@url}?since=#{last + 1}").body |> Poison.decode!
    new_last = String.to_integer(tid)
    write_csv(resp, last, new_last)
    Process.send_after(self(), :tick, @period)
    new_last
  end

  defp write_csv(resp, last_tid, new_last_tid) do
    Logger.debug("writing #{last_tid}:#{new_last_tid}")

    resp
    |> Stream.map(&flatten/1)
    |> Enum.join("\n")
    |> History.CSV.write("cex2", "#{last_tid}:#{new_last_tid}")
  end

  defp flatten(%{"amount" => amount, "date" => date, "price" => price,
                 "tid" => tid, "type" => type}) do
    "#{type},#{date},#{amount},#{price},#{tid}"
  end
end

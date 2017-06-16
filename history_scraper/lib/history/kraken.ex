defmodule History.Kraken do
  use GenServer
  require Logger

  @period 2_000
  @url "https://api.kraken.com/0/public/Trades?pair=XXBTZUSD"

  def start_link(last) do
    GenServer.start_link(__MODULE__, last, name: __MODULE__)
  end


  def init(last) do
    Process.send_after(self(), :tick, @period)
    {:ok, last}
  end

  defp initial_request do
    %{"error" => [], "result" => %{"XXBTZUSD" => resp, "last" => last}} =
      HTTPoison.get!("#{@url}&since=0").body |> Poison.decode!

    write_csv(resp, 0, last)
    last
  end

  def handle_info(:tick, last) do
    %{"error" => [], "result" => %{"XXBTZUSD" => resp, "last" => new_last}} =
      HTTPoison.get!("#{@url}&since=#{last}").body |> Poison.decode!

    write_csv(resp, last, new_last)

    Process.send_after(self(), :tick, @period)
    {:noreply, new_last}
  end


  defp write_csv(resp, old_last, new_last) do
    Logger.debug("writing #{old_last}:#{new_last}")

    resp
    |> Stream.map(&Enum.join(&1, ","))
    |> Enum.join("\n")
    |> History.CSV.write("kraken", "#{old_last}:#{new_last}")
  end
end

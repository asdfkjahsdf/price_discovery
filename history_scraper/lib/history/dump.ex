defmodule History.Dump do
  require Logger

  def save(venue, data) do
    Logger.debug("saving dump #{venue} with data #{data}")
    File.write("priv/dump/#{venue}", :erlang.term_to_binary(data))
  end

  def read(venue) do
    case File.read("priv/dump/#{venue}") do
      {:ok, binary} ->
        File.rm("priv/dump/#{venue}")
        {:ok, :erlang.binary_to_term(binary)}
      error -> Logger.error("couldn't read dump: #{inspect(error)}")
    end
  end
end

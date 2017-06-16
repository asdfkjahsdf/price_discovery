defmodule History.CSV do

  def write(data, venue, filename) do
    File.write("priv/#{venue}/#{filename}", data)
  end
end

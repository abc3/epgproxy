defmodule Epgproxy do
  require Logger

  def db_call(bin) do
    :poolboy.transaction(
      :db_sess,
      fn pid ->
        try do
          Epgproxy.DbSess2.call(pid, bin)
        catch
          e, r ->
            Logger.debug("poolboy transaction caught error: #{inspect(e)}, #{inspect(r)}")
            :ok
        end
      end,
      12000
    )
  end

  def client_call(pid, bin) do
    :ok
  end
end

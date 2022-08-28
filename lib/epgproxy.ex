defmodule Epgproxy do
  require Logger

  def db_call(bin) do
    :poolboy.transaction(
      :db_sess,
      fn pid ->
        try do
          Epgproxy.DbSess.call(pid, bin)
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

  def start() do
    host =
      Application.get_env(:epgproxy, :db_host)
      |> String.to_charlist()

    creds = %{
      host: host,
      port: Application.get_env(:epgproxy, :db_port),
      user: Application.get_env(:epgproxy, :db_user),
      database: Application.get_env(:epgproxy, :db_name),
      password: Application.get_env(:epgproxy, :db_password),
      application_name: Application.get_env(:epgproxy, :application_name)
    }

    case DBConnection.start_link(Epgproxy.ClientProtocol,
           creds: creds,
           show_sensitive_data_on_connection_error: true
         ) do
      {:ok, pid} ->
        :syn.register(:conn, "dev", pid)

      other ->
        Logger.debug("db connection failed", msg: other)
        {:error, other}
    end
  end

  def restart() do
    case :syn.lookup(:conn, "dev") do
      :undefined ->
        nil

      {pid, _} ->
        stop(pid)
    end

    start()
  end

  def stop(pid) do
    GenServer.stop(pid)
  end
end

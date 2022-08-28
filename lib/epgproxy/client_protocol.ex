defmodule Epgproxy.ClientProtocol do
  @moduledoc false

  require Logger
  use DBConnection

  alias Epgproxy.Proto.Server

  @impl true
  def connect(opts) do
    Logger.debug("Connecting to DB", msg: inspect(opts, pretty: true))
    connect_endpoints(opts[:creds])
  end

  def connect_endpoints(creds) do
    socket_opts = [:binary, {:packet, :raw}, {:active, false}]

    case :gen_tcp.connect(creds.host, creds.port, socket_opts) do
      {:ok, socket} ->
        Logger.debug("creds #{inspect(creds, pretty: true)}")
        :ok = authentication(socket, creds)
        {:ok, %{socket: socket}}

      other ->
        Logger.error("Connection faild}", msg: other)
        {:error, other}
    end
  end

  def authentication(socket, creds) do
    msg =
      :pgo_protocol.encode_startup_message([
        {"user", creds.user},
        {"database", creds.database},
        # {"password", creds.password},
        {"application_name", creds.application_name}
      ])

    :ok = :gen_tcp.send(socket, msg)

    # TODO: implement other authentication methods
    case :gen_tcp.recv(socket, 1, 1_000) do
      {:ok, <<?R>>} ->
        dec_pkt =
          (<<?R>> <> recv_once(socket))
          |> Server.decode()

        Logger.debug("dec_pkt, #{inspect(dec_pkt, pretty: true)}")
        :ok

      {:error, reason} ->
        Logger.error("authentication err", msg: reason)
    end
  end

  def recv_once(socket, timeout \\ 5000) do
    :inet.setopts(socket, [{:active, :once}])

    receive do
      {:tcp, ^socket, buffer} ->
        buffer

      {:tcp_closed, ^socket} ->
        Logger.error("socket closed")
        nil

      {:tcp_error, ^socket, reason} ->
        Logger.error("socket error", msg: reason)
        nil
    after
      timeout ->
        Logger.error("recv_once timeout", msg: timeout)
        nil
    end
  end

  @impl true
  def disconnect(_, _s) do
    :ok
  end

  @impl true
  @spec ping(map()) :: {:ok, map()}
  def ping(state) do
    # Logger.debug("ping", msg: state)
    {:ok, state}
  end

  @impl true
  def checkout(state) do
    Logger.debug("checkout", msg: state)
    {:ok, state}
  end

  def checkin(state) do
    Logger.debug("checkin", msg: state)
    {:ok, state}
  end
end

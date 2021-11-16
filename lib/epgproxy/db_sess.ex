defmodule Epgproxy.DbSess do
  use GenServer
  require Logger
  alias Epgproxy.Proto
  alias Epgproxy.Proto.Pkt

  @pkt_header_size 5

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_) do
    auth = %{
      host: {127, 0, 0, 1},
      port: 5432,
      user: "user",
      database: "postgres",
      application_name: "epgproxy"
    }

    {:ok, %{socket: nil, caller: nil, sent: false, auth: auth, payload_size: 0, buffer: <<>>},
     {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, %{auth: auth} = state) do
    socket_opts = [:binary, {:packet, :raw}, {:active, false}]
    Logger.debug("DbSess auth #{inspect(auth)}")

    case :gen_tcp.connect(auth.host, auth.port, socket_opts) do
      {:ok, socket} ->
        msg =
          :pgo_protocol.encode_startup_message([
            {"user", auth.user},
            {"database", auth.database},
            {"user", auth.user},
            {"application_name", auth.application_name}
          ])

        :ok = :gen_tcp.send(socket, msg)
        :ok = active_once(socket)
        {:noreply, %{state | socket: socket}}

      other ->
        Logger.error("Connection faild #{inspect(other)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:tcp, _port, data}, %{sent: true, caller: caller} = state) do
    Logger.debug("Resend data")
    GenServer.call(caller, {:reponse, data})
    {:noreply, %{state | sent: false}}
  end

  def handle_info({:tcp_closed, _port}, %{socket: socket} = state) do
    Logger.debug("Port is closed")
    :ok = :gen_tcp.close(socket)
    {:stop, :normal, state}
  end

  def handle_info({:tcp, _port, data}, %{buffer: buf} = state) do
    Logger.debug("Got data #{inspect(byte_size(data))} bytes")
    dec_pkt = handle_response(buf <> data, [])
    Logger.debug("Decoded #{inspect(dec_pkt)}")
    {:noreply, %{state | buffer: <<>>}}
  end

  @impl true
  def handle_call({:db, msg}, {caller, _}, %{socket: socket} = state) do
    Logger.debug("db call, caller: #{inspect(caller)}")
    :gen_tcp.send(socket, msg)
    {:reply, :ok, %{state | caller: caller, sent: true}}
  end

  def active_once(socket) do
    :inet.setopts(socket, [{:active, :once}])
  end

  def handle_response(data, acc) when byte_size(data) >= 5 do
    {:ok, pkt, rest} = Proto.decode(data)
    handle_response(rest, [pkt | acc])
  end

  def handle_response(_, acc) do
    acc
  end
end

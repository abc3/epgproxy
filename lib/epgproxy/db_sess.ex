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
      user: "postgres",
      database: "postgres",
      application_name: "epgproxy"
    }

    {:ok,
     %{
       socket: nil,
       caller: nil,
       sent: false,
       auth: auth,
       payload_size: 0,
       buffer: <<>>,
       db_state: nil,
       parameter_status: %{},
       wait: false
     }, {:continue, :connect}}
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
    {:stop, :normal, %{state | db_state: nil}}
  end

  def handle_info({:tcp, _port, data}, %{buffer: buf, db_state: nil, socket: socket} = state) do
    Logger.debug("Got data #{inspect(byte_size(data))} bytes")
    dec_pkt = Proto.decode(buf <> data)
    IO.inspect({:decoded, dec_pkt})

    {ps, db_state} =
      Enum.reduce(dec_pkt, {%{}, nil}, fn
        %{tag: :parameter_status, payload: {k, v}}, {ps, db_state} ->
          {Map.put(ps, k, v), db_state}

        %{tag: :ready_for_query, payload: db_state}, {ps, _} ->
          {ps, db_state}

        _e, acc ->
          acc
      end)

    :gen_tcp.send(socket, Proto.test_query())
    active_once(socket)

    {:noreply, %{state | buffer: <<>>, db_state: db_state, parameter_status: ps, wait: true}}
  end

  def handle_info({:tcp, _port, data}, %{wait: true, socket: socket} = state) do
    IO.inspect({:data, data})
    dec_pkt = Proto.decode(data)
    IO.inspect({:decoded, dec_pkt})

    active_once(socket)
    {:noreply, %{state | buffer: <<>>}}
  end

  def handle_info({:tcp, _port, data}, %{db_state: :idle} = state) do
    IO.inspect({:data, data})
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
end

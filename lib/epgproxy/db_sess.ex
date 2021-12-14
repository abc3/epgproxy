defmodule Epgproxy.DbSess do
  require Logger
  use GenServer
  alias Epgproxy.Proto.Server

  @connect_timeout Application.get_env(:epgproxy, :connect_timeout)

  def start_link(config) when is_list(config) do
    GenServer.start_link(__MODULE__, config)
  end

  def call(pid, msg) do
    GenServer.call(pid, {:db_call, msg})
  end

  @impl true
  def init(_) do
    {:ok, host} =
      Application.get_env(:epgproxy, :db_host)
      |> String.to_charlist()
      |> :inet.parse_address()

    auth = %{
      host: host,
      port: Application.get_env(:epgproxy, :db_port),
      user: Application.get_env(:epgproxy, :db_user),
      database: Application.get_env(:epgproxy, :db_name),
      application_name: Application.get_env(:epgproxy, :application_name)
    }

    state = %{
      check_ref: make_ref(),
      socket: nil,
      caller: nil,
      sent: false,
      auth: auth,
      payload_size: 0,
      buffer: <<>>,
      db_state: nil,
      parameter_status: %{},
      wait: false,
      stage: nil
    }

    Registry.register(Registry.EpgproxyStats, "db_sess_size", System.system_time(:second))
    send(self(), :connect)
    {:ok, state}
  end

  # receive call from the client
  @impl true
  def handle_call({:db_call, bin}, {pid, _ref} = _from, %{socket: socket} = state) do
    Logger.debug("<-- <-- bin #{inspect(byte_size(bin))} bytes, caller: #{inspect(pid)}")
    :gen_tcp.send(socket, bin)
    {:reply, :ok, %{state | caller: pid}}
  end

  @impl true
  def handle_info(:connect, %{auth: auth, check_ref: ref} = state) do
    Logger.info("Try to connect to DB")
    Process.cancel_timer(ref)
    socket_opts = [:binary, {:packet, :raw}, {:active, true}]

    case :gen_tcp.connect(auth.host, auth.port, socket_opts) do
      {:ok, socket} ->
        Logger.debug("auth #{inspect(auth, pretty: true)}")

        msg =
          :pgo_protocol.encode_startup_message([
            {"user", auth.user},
            {"database", auth.database},
            # {"password", auth.user},
            {"application_name", auth.application_name}
          ])

        :ok = :gen_tcp.send(socket, msg)
        {:noreply, %{state | stage: :authentication, socket: socket}}

      other ->
        Logger.error("Connection faild #{inspect(other)}")
        {:noreply, %{state | check_ref: reconnect()}}
    end
  end

  def handle_info({:tcp, _port, bin}, %{stage: :authentication} = state) do
    dec_pkt = Server.decode(bin)
    Logger.debug("dec_pkt, #{inspect(dec_pkt, pretty: true)}")

    {ps, db_state} =
      Enum.reduce(dec_pkt, {%{}, nil}, fn
        %{tag: :parameter_status, payload: {k, v}}, {ps, db_state} ->
          {Map.put(ps, k, v), db_state}

        %{tag: :ready_for_query, payload: db_state}, {ps, _} ->
          {ps, db_state}

        _e, acc ->
          acc
      end)

    Logger.debug("parameter_status: #{inspect(ps, pretty: true)}")
    Logger.debug("DB ready_for_query: #{inspect(db_state)}")
    {:noreply, %{state | parameter_status: ps, stage: :idle}}
  end

  # receive reply from DB and send to the client
  def handle_info({:tcp, _port, bin}, %{caller: caller, buffer: buf} = state) do
    Logger.debug("--> bin #{inspect(byte_size(bin))} bytes")

    case handle_packets(buf <> bin) do
      {:ok, :ready_for_query, rest, :idle} ->
        Epgproxy.ClientSess.client_call(caller, bin, true)
        # :poolboy.checkin(:db_sess, self())
        {:noreply, %{state | buffer: rest}}

      {:ok, _, rest, _} ->
        Epgproxy.ClientSess.client_call(caller, bin, false)
        {:noreply, %{state | buffer: rest}}
    end
  end

  def handle_info({:tcp_closed, _port}, state) do
    Logger.error("DB closed connection")
    {:noreply, %{state | check_ref: reconnect()}}
  end

  def handle_info(msg, state) do
    msg = [
      {"msg", msg},
      {"state", state}
    ]

    Logger.error("Undefined msg: #{inspect(msg, pretty: true)}")
    {:noreply, state}
  end

  def terminate(_reason, _state, _data) do
    Logger.debug("DB terminated")
    :ok
  end

  def handle_packets(<<char::integer-8, pkt_len::integer-32, rest::binary>> = bin) do
    payload_len = pkt_len - 4
    tag = Server.tag(char)

    case rest do
      <<payload::binary-size(payload_len)>> ->
        pkt = Server.packet(tag, pkt_len, payload)
        Logger.debug(inspect(pkt, pretty: true))

        {:ok, tag, "", pkt.payload}

      <<payload::binary-size(payload_len), rest1::binary>> ->
        pkt = Server.packet(tag, pkt_len, payload)
        Logger.debug(inspect(pkt, pretty: true))

        handle_packets(rest1)

      _ ->
        {:ok, tag, bin, ""}
    end
  end

  def handle_packets(bin) do
    {:ok, :small_chunk, bin, ""}
  end

  def reconnect() do
    Process.send_after(self(), :connect, @connect_timeout)
  end

  def send_active_once(socket, msg) do
    :gen_tcp.send(socket, msg)
    :inet.setopts(socket, [{:active, :once}])
  end

  def active_once(socket) do
    :inet.setopts(socket, [{:active, :once}])
  end
end

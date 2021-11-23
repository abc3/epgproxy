defmodule Epgproxy.DbSess do
  require Logger
  @behaviour :gen_statem
  alias Epgproxy.Proto.Server

  def start_link(_) do
    :gen_statem.start_link(__MODULE__, [], [])
  end

  def call(msg) do
    :gen_statem.call(__MODULE__, {:db_call, msg})
  end

  def call(pid, msg) do
    :gen_statem.call(pid, {:db_call, msg})
  end

  def server_call(msg) do
    :gen_statem.call(__MODULE__, {:db_call, msg})
  end

  @impl true
  def callback_mode(), do: [:handle_event_function]

  @impl true
  def init(_) do
    auth = %{
      host: {127, 0, 0, 1},
      port: 5432,
      user: "postgres",
      database: "postgres",
      application_name: "epgproxy"
    }

    data = %{
      socket: nil,
      caller: nil,
      sent: false,
      auth: auth,
      payload_size: 0,
      buffer: <<>>,
      db_state: nil,
      parameter_status: %{},
      wait: false
    }

    {:ok, :db_connect, data, [{:next_event, :internal, :ok}]}
  end

  @impl true
  def handle_event(:internal, _, :db_connect, %{auth: auth} = data) do
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
        {:next_state, :authentication, %{data | socket: socket}}

      other ->
        Logger.error("Connection faild #{inspect(other)}")
        {:stop, :normal}
    end
  end

  # receive call from the client
  def handle_event({:call, {pid, _ref} = from}, {:db_call, bin}, _, %{socket: socket} = data) do
    Logger.debug("<-- <-- bin #{inspect(byte_size(bin))} bytes")
    :gen_tcp.send(socket, bin)
    {:keep_state, %{data | caller: pid}, [{:reply, from, :ok}]}
  end

  def handle_event(:info, {:tcp, _port, bin}, :authentication, data) do
    dec_pkt = Server.decode(bin)

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
    {:next_state, :idle, %{data | parameter_status: ps}}
  end

  # receive reply from DB and send to the client
  def handle_event(
        :info,
        {:tcp, _port, bin},
        :idle,
        %{
          caller: caller,
          buffer: buf
        } = data
      ) do
    Logger.debug("--> bin #{inspect(byte_size(bin))} bytes")

    Epgproxy.ClientSess.client_call(caller, bin)

    case handle_packets(buf <> bin) do
      {:ok, :ready_for_query, _} ->
        :poolboy.checkin(:db_sess, self())
        {:keep_state, %{data | buffer: <<>>}}

      {:ok, _, rest} ->
        {:keep_state, %{data | buffer: rest}}
    end
  end

  def handle_event(:info, {:tcp_closed, _port}, _, _) do
    Logger.error("DB closed connection")
    :keep_state_and_data
  end

  def handle_event(event_type, event_content, state, data) do
    msg = [
      {"event_type", event_type},
      {"event_content", event_content},
      {"state", state},
      {"data", data}
    ]

    Logger.error("Undefined msg: #{inspect(msg, pretty: true)}")
    :keep_state_and_data
  end

  @impl true
  def terminate(_reason, _state, _data) do
    Logger.debug("DB terminated")
    :ok
  end

  def handle_packets(<<char::integer-8, pkt_len::integer-32, rest::binary>> = bin) do
    payload_len = pkt_len - 4
    tag = Server.tag(char)

    case rest do
      <<payload::binary-size(payload_len)>> ->
        Logger.debug(inspect(Server.packet(tag, pkt_len, payload), pretty: true))

        {:ok, tag, ""}

      <<payload::binary-size(payload_len), rest1::binary>> ->
        Logger.debug(inspect(Server.packet(tag, pkt_len, payload), pretty: true))

        handle_packets(rest1)

      _ ->
        {:ok, tag, bin}
    end
  end

  def handle_packets(bin) do
    {:ok, :small_chunk, bin}
  end

  def send_active_once(socket, msg) do
    :gen_tcp.send(socket, msg)
    :inet.setopts(socket, [{:active, :once}])
  end

  def active_once(socket) do
    :inet.setopts(socket, [{:active, :once}])
  end
end

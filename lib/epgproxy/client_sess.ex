defmodule Epgproxy.ClientSess do
  require Logger
  use GenServer
  @behaviour :ranch_protocol

  alias Epgproxy.{Proto.Client, Cache}

  @impl true
  def start_link(ref, _socket, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, transport, opts])
    Registry.register(Registry.EpgproxyStats, "client_sess_size", System.system_time(:second))
    {:ok, pid}
  end

  def client_call(pid, bin, ready?) do
    GenServer.call(pid, {:client_call, bin, ready?})
  end

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  def init(ref, trans, _opts) do
    {:ok, socket} = :ranch.handshake(ref)
    :ok = trans.setopts(socket, [{:active, true}])
    Logger.info("Epgproxy.ClientSess is: #{inspect(self())}")

    :gen_server.enter_loop(
      __MODULE__,
      [],
      %{
        socket: socket,
        trans: trans,
        connected: false,
        pgo: nil,
        buffer: <<>>,
        db_pid: nil,
        stage: :wait_startup_packet
      }
    )
  end

  @impl true
  def handle_info(
        {:tcp, _port, bin},
        %{trans: trans, socket: socket, stage: :wait_startup_packet} = state
      ) do
    Logger.debug("Startup <-- bin #{inspect(byte_size(bin))}")

    # SSL negotiation, S/N/Error
    if byte_size(bin) == 8 do
      trans.send(socket, "N")
      {:noreply, state}
    else
      hello = Client.decode_startup_packet(bin)
      Logger.debug("Client startup message: #{inspect(hello)}")
      trans.send(socket, authentication_ok())
      {:noreply, %{state | stage: :idle}}
    end
  end

  def handle_info({:tcp, _port, <<"X", 0, 0, 0, 4>>}, state) do
    Logger.debug("Exclude termination")
    {:noreply, state}
  end

  def handle_info({:tcp, _port, bin1}, %{buffer: buf, db_pid: db_pid} = state) do
    db_pid1 =
      if db_pid do
        db_pid
      else
        :poolboy.checkout(:db_sess)
      end

    Logger.debug("Worker: #{inspect(db_pid1)}")

    {rest, _, _transaction} =
      Client.stream(buf <> bin1)
      |> Enum.reduce(
        {<<>>, db_pid1, nil},
        fn
          {:rest, rest}, {_, db_pid, transaction} ->
            {rest, db_pid, transaction}

          pkt, {_, db_pid, _} = acc ->
            if pkt.tag == :simple_query do
              IO.inspect({"simple_query :: " <> pkt.payload})
              GenServer.call(db_pid, {:db_call, pkt.bin, {:cache, pkt.payload}})
            else
              Epgproxy.DbSess.call(db_pid, pkt.bin)
            end

            Logger.debug(inspect(%{pkt | bin: ""}, pretty: true))
            # Epgproxy.DbSess.call(db_pid, pkt.bin)
            acc
        end
      )

    Logger.debug("rest #{inspect(rest, pretty: true)}")

    {:noreply, %{state | buffer: rest, db_pid: db_pid1}}
  end

  def handle_info({:tcp_closed, _port}, state) do
    Logger.info("Client closed connection")
    {:stop, :normal, state}
  end

  def handle_info(msg, state) do
    msg = [
      {"msg", msg},
      {"state", state}
    ]

    Logger.error("Undefined msg: #{inspect(msg, pretty: true)}")

    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:client_call, bin, ready?},
        _,
        %{socket: socket, trans: trans, db_pid: db_pid} = state
      ) do
    db_pid1 =
      if ready? do
        :poolboy.checkin(:db_sess, db_pid)
        nil
      else
        db_pid
      end

    Logger.debug("--> --> bin #{inspect(byte_size(bin))} bytes")
    trans.send(socket, bin)
    {:reply, :ok, %{state | db_pid: db_pid1}}
  end

  def test_conn() do
    # {:ok, pid} =
    #   Postgrex.start_link(
    #     hostname: "localhost",
    #     username: "postgres",
    #     # password: "postgres",
    #     database: "postgres",
    #     port: 5555
    #   )

    # pid
    # {:ok, #PID<0.69.0>}
    #     :epgsql.connect(%{
    #       # :port => 5555,
    #       :host => 'localhost',
    #       :database => 'postgres',
    #       :user => 'postgres',
    #       :password => 'postgres'
    #     })

    # pid
    :pgo.start_pool(:default, %{
      :pool_size => 1,
      :port => 5555,
      :host => "127.0.0.1",
      :database => "postgres",
      :user => "postgres"
    })
  end

  def send_active_once(trans, socket, msg) do
    trans.send(socket, msg)
    :inet.setopts(socket, [{:active, :once}])
  end

  def active_once(socket) do
    :inet.setopts(socket, [{:active, :once}])
  end

  def authentication_ok() do
    [
      # authentication_ok
      <<"R", 0, 0, 0, 8>>,
      <<0, 0, 0, 0>>,
      # parameter_status,<<"application_name">>,<<"nonode@nohost">>
      <<83, 0, 0, 0, 35>>,
      <<97, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 95, 110, 97, 109, 101, 0, 110, 111,
        110, 111, 100, 101, 64, 110, 111, 104, 111, 115, 116, 0>>,
      # parameter_status,<<"client_encoding">>,<<"UTF8">>
      <<83, 0, 0, 0, 25>>,
      <<99, 108, 105, 101, 110, 116, 95, 101, 110, 99, 111, 100, 105, 110, 103, 0, 85, 84, 70, 56,
        0>>,
      # parameter_status,<<"server_version">>,<<"14.1">>
      <<83, 0, 0, 0, 24>>,
      <<115, 101, 114, 118, 101, 114, 95, 118, 101, 114, 115, 105, 111, 110, 0, "14.1", 0>>,
      # parameter_status,<<"session_authorization">>,<<"postgres">>
      <<83, 0, 0, 0, 35>>,
      <<115, 101, 115, 115, 105, 111, 110, 95, 97, 117, 116, 104, 111, 114, 105, 122, 97, 116,
        105, 111, 110, 0, 112, 111, 115, 116, 103, 114, 101, 115, 0>>,
      # parameter_status,<<"standard_conforming_strings">>,<<"on">>
      <<83, 0, 0, 0, 35>>,
      <<115, 116, 97, 110, 100, 97, 114, 100, 95, 99, 111, 110, 102, 111, 114, 109, 105, 110, 103,
        95, 115, 116, 114, 105, 110, 103, 115, 0, 111, 110, 0>>,
      # parameter_status,<<"TimeZone">>,<<"Europe/Kiev">>
      <<83, 0, 0, 0, 25>>,
      <<84, 105, 109, 101, 90, 111, 110, 101, 0, 69, 117, 114, 111, 112, 101, 47, 75, 105, 101,
        118, 0>>,
      # backend_key_data,59194,2347138713
      <<75, 0, 0, 0, 12>>,
      <<0, 0, 231, 58, 139, 230, 126, 153>>,
      # ready_for_query,idle
      <<90, 0, 0, 0, 5>>,
      <<"I">>
    ]
  end
end

defmodule Epgproxy.ClientSess do
  require Logger
  @behaviour :gen_statem
  @behaviour :ranch_protocol

  alias Epgproxy.Proto.Client

  @impl true
  def start_link(ref, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  def client_call(pid, msg) do
    :gen_statem.call(pid, {:client_call, msg})
  end

  @impl true
  def callback_mode(), do: [:handle_event_function]

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  def init(ref, trans, _opts) do
    {:ok, socket} = :ranch.handshake(ref)
    :ok = trans.setopts(socket, [{:active, true}])
    Logger.info("Epgproxy.ClientSess is: #{inspect(self())}")

    :gen_statem.enter_loop(
      __MODULE__,
      [],
      # :idle,
      :wait_startup_packet,
      %{
        socket: socket,
        trans: trans,
        connected: false,
        db_sess: nil,
        pgo: nil
      }
    )
  end

  def handle_event(
        :info,
        {:tcp, _port, bin},
        :wait_startup_packet,
        %{trans: trans, socket: socket} = data
      ) do
    Logger.debug("Startup <-- bin #{inspect(byte_size(bin))}")
    # IO.inspect(bin, limit: :infinity)

    # SSL negotiation, S/N/Error
    if byte_size(bin) == 8 do
      trans.send(socket, "N")
      :keep_state_and_data
    else
      hello = Client.decode_startup_packet(bin)
      Logger.debug("Client startup message: #{inspect(hello)}")
      trans.send(socket, authentication_ok())
      {:next_state, :idle, data}
    end
  end

  def handle_event(
        {:call, from},
        {:client_call, bin},
        :wait_startup_packet,
        %{
          socket: socket,
          trans: trans
        } = data
      ) do
    Logger.debug("Startup --> --> bin #{inspect(byte_size(bin))} bytes")
    trans.send(socket, bin)
    {:next_state, :idle, data, [{:reply, from, :ok}]}
  end

  ##############################################################################

  def handle_event(:info, {:tcp, _port, <<?Q, _::binary>> = bin}, :idle, data) do
    db_sess = :poolboy.checkout(:db_sess)
    Logger.debug("<-- bin #{inspect(byte_size(bin))} bytes / db_sess #{inspect(db_sess)}")

    IO.inspect({:from_client, Epgproxy.Proto.Client.decode(bin)})
    Epgproxy.DbSess.call(db_sess, bin)
    :keep_state_and_data
  end

  # def handle_event(:info, {:tcp, _port, bin}, :idle, data) do
  #   Logger.debug("<-- bin #{inspect(byte_size(bin))} bytes")
  #   IO.inspect({:from_client, Epgproxy.Proto.Client.decode(bin)})
  #   # Epgproxy.DbSess2.server_call(bin)
  #   Epgproxy.db_call(bin)
  #   :keep_state_and_data
  # end

  def handle_event({:call, from}, {:client_call, bin}, _, %{socket: socket, trans: trans}) do
    Logger.debug("--> --> bin #{inspect(byte_size(bin))} bytes")
    trans.send(socket, bin)
    {:keep_state_and_data, [{:reply, from, :ok}]}
  end

  def handle_event(:info, {:tcp_closed, _port}, _, _) do
    Logger.error("Client closed connection")
    {:stop, :normal}
  end

  # %%%%%%%%%%%%%%5

  # @impl true
  # def handle_event(
  #       :info,
  #       {:tcp, _port, bin},
  #       :wait_startup_packet,
  #       %{
  #         socket: socket,
  #         trans: trans
  #       } = data
  #     ) do
  #   hello = Proto.decode_startup_packet(bin)
  #   IO.inspect({:hello, hello})
  #   trans.send(socket, authentication_ok())
  #   {:next_state, :connected, data}
  # end

  # def handle_event(:info, {:tcp, _port, bin}, :connected, _) do
  #   dec = Epgproxy.Proto.Client.decode(bin)
  #   IO.inspect({:indle, dec})
  #   :ok = Epgproxy.DbSess.call(bin)
  #   :keep_state_and_data
  # end

  # def handle_event(
  #       {:call, from},
  #       {:reply, bin},
  #       _,
  #       %{
  #         socket: socket,
  #         trans: trans
  #       }
  #     ) do
  #   # Logger.debug("Reply")
  #   Logger.debug("Client proxy bin #{inspect(byte_size(bin))} bytes")
  #   trans.send(socket, bin)
  #   {:keep_state_and_data, [{:reply, from, :ok}]}
  # end

  # def handle_event(:info, {:tcp_closed, _port}, _, _) do
  #   IO.inspect(:tcp_closed)
  #   {:stop, :normal}
  # end

  def handle_event(event_type, event_content, state, data) do
    IO.inspect([
      {"event_type", event_type},
      {"event_content", event_content},
      {"state", state},
      {"data", data}
    ])

    :keep_state_and_data
  end

  def test_conn() do
    {:ok, pid} =
      Postgrex.start_link(
        hostname: "localhost",
        username: "postgres",
        # password: "postgres",
        database: "postgres",
        port: 5555
      )

    pid
    # {:ok, #PID<0.69.0>}
    #     :epgsql.connect(%{
    #       # :port => 5555,
    #       :host => 'localhost',
    #       :database => 'postgres',
    #       :user => 'postgres',
    #       :password => 'postgres'
    #     })

    # pid
    # :pgo.start_pool(:default, %{
    #   :pool_size => 1,
    #   :port => 5555,
    #   :host => "127.0.0.1",
    #   :database => "postgres",
    #   :user => "postgres"
    # })
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
      # parameter_status,<<"server_version">>,<<"13.3">>
      <<83, 0, 0, 0, 24>>,
      <<115, 101, 114, 118, 101, 114, 95, 118, 101, 114, 115, 105, 111, 110, 0, 49, 51, 46, 51,
        0>>,
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

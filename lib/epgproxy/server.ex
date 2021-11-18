defmodule Epgproxy.Server do
  require Logger
  @behaviour :gen_statem
  @behaviour :ranch_protocol

  alias Epgproxy.Proto

  @impl true
  def start_link(ref, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  @impl true
  def callback_mode() do
    [
      # :state_enter,
      :handle_event_function
    ]
  end

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
      :wait_startup_packet,
      %{
        socket: socket,
        trans: trans,
        connected: false,
        pgo: nil
      }
    )
  end

  @impl true
  def handle_event(
        :info,
        {:tcp, _port, bin},
        :wait_startup_packet,
        %{
          socket: socket,
          trans: trans
        } = data
      ) do
    hello = Proto.decode_startup_packet(bin)
    IO.inspect({:hello, hello})
    trans.send(socket, authentication_ok())
    {:next_state, :idle, data}
  end

  def handle_event(:info, {:tcp, _port, bin}, :idle, _) do
    dec = Epgproxy.Proto.Client.decode(bin)
    IO.inspect({:indle, dec})
    :ok = Epgproxy.DbSess.call(bin)
    :keep_state_and_data
  end

  def handle_event(
        {:call, from},
        {:reply, bin},
        _,
        %{
          socket: socket,
          trans: trans
        }
      ) do
    Logger.debug("Reply")
    trans.send(socket, bin)
    {:keep_state_and_data, [{:reply, from, :ok}]}
  end

  def handle_event(:info, {:tcp_closed, _port}, _, _) do
    IO.inspect(:tcp_closed)
    {:stop, :normal}
  end

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
    :pgo.start_pool(:default, %{
      :pool_size => 1,
      :port => 5555,
      :host => "127.0.0.1",
      :database => "postgres",
      :user => "postgres"
    })
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

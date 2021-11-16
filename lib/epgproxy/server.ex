defmodule Epgproxy.Server do
  use GenServer
  require Logger
  @behaviour :ranch_protocol

  @impl true
  def start_link(ref, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  def init(ref, trans, _opts) do
    {:ok, socket} = :ranch.handshake(ref)
    :ok = trans.setopts(socket, [{:active, true}])
    Logger.info("Epgproxy.Server is: #{inspect(self())}")

    :gen_server.enter_loop(__MODULE__, [], %{
      socket: socket,
      trans: trans,
      connected: false,
      pgo: nil
    })
  end

  @impl true
  def handle_info({:tcp, _pid, _data}, %{connected: false, trans: trans, socket: socket} = state) do
    trans.send(socket, authentication_ok())
    {:noreply, %{state | connected: true}}
  end

  # def handle_info({:tcp, _, <<"Q", _len::integer-32, rest::binary>>}, state) do
  #   IO.inspect({"got", String.slice(rest, 0..-2), byte_size(rest)}, limit: :infinity)
  #   {:noreply, state}
  # end

  def handle_info({:tcp, _, data}, state) do
    IO.inspect({"Server got", data, byte_size(data)}, limit: :infinity)
    GenServer.call(Epgproxy.DbSess, {:db, data})
    {:noreply, state}
  end

  def handle_info({:tcp_closed, _}, _state) do
    {:stop, :normal}
  end

  def handle_info(msg, state) do
    IO.inspect({"got", msg, self()})
    {:noreply, state}
  end

  @impl true
  def handle_call({:reponse, msg}, _, %{trans: trans, socket: socket} = state) do
    Logger.info("{:reponse, msg}")
    trans.send(socket, msg)
    {:reply, :ok, state}
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

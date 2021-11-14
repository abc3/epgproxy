defmodule Epgproxy.Proxy do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, %{socket: nil, caller: nil, sent: false}, {:continue, :auth}}
  end

  @impl true
  def handle_continue(:auth, state) do
    case :gen_tcp.connect({127, 0, 0, 1}, 5432, [:binary, {:packet, :raw}, {:active, true}]) do
      {:ok, socket} ->
        msg =
          :pgo_protocol.encode_startup_message([
            {"user", "postgres"},
            {"database", "postgres"},
            {"user", "postgres"},
            {"application_name", "qweqwe"}
          ])

        :ok = :gen_tcp.send(socket, msg)
        {:noreply, %{state | socket: socket}}

      other ->
        Logger.error("Connection faild #{inspect(other)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:tcp, _port, data}, %{sent: true, caller: caller} = state) do
    Logger.info("Resend data")
    GenServer.call(caller, {:reponse, data})
    {:noreply, %{state | sent: false}}
  end

  def handle_info({:tcp, _port, data}, state) do
    Logger.info("Proxy got data: #{inspect(data)}")
    {:noreply, state}
  end

  @impl true
  def handle_call({:db, msg}, {caller, _}, %{socket: socket} = state) do
    Logger.debug("db call, caller: #{inspect(caller)}")
    :gen_tcp.send(socket, msg)
    {:reply, :ok, %{state | caller: caller, sent: true}}
  end
end

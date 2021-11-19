defmodule Epgproxy.Proto.Client do
  require Logger

  @pkt_header_size 5

  defmodule(Pkt,
    do: defstruct([:tag, :len, :payload])
  )

  def decode(data) do
    decode(data, [])
  end

  def decode(data, acc) when byte_size(data) >= @pkt_header_size do
    {:ok, pkt, rest} = decode_pkt(data)
    decode(rest, [pkt | acc])
  end

  def decode(_, acc) do
    Enum.reverse(acc)
  end

  def decode_pkt(<<char::integer-8, pkt_len::integer-32, rest::binary>>, decode_payload \\ true) do
    tag = tag(char)
    payload_len = pkt_len - 4

    <<bin_payload::binary-size(payload_len), rest2::binary>> = rest

    payload =
      if decode_payload do
        decode_payload(tag, bin_payload)
      else
        nil
      end

    {:ok, %Pkt{tag: tag, len: pkt_len + 1, payload: payload}, rest2}
  end

  def tag(char) do
    case char do
      ?Q -> :simple_query
      _ -> :undefined
    end
  end

  def decode_payload(:simple_query, payload) do
    case String.split(payload, <<0>>) do
      [query, ""] -> query
      _ -> :undefined
    end
  end

  def decode_startup_packet(<<len::integer-32, _protocol::binary-4, rest::binary>>) do
    # <<major::integer-16, minor::integer-16>> = protocol

    %Pkt{
      len: len,
      payload: String.split(rest, <<0>>, trim: true) |> Enum.chunk_every(2),
      tag: :statup
    }
  end
end

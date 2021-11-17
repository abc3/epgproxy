defmodule Epgproxy.Proto do
  require Logger

  defmacro pkt_header_size, do: 5

  defmodule(Pkt,
    do: defstruct([:tag, :len, :payload])
  )

  def decode(<<char::integer-8, pkt_len::integer-32, rest::binary>>, decode_payload \\ true) do
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
      ?R -> :authentication
      ?K -> :backend_key_data
      ?2 -> :bind_complete
      ?3 -> :close_complete
      ?C -> :command_complete
      ?d -> :copy_data
      ?c -> :copy_done
      ?G -> :copy_in_response
      ?H -> :copy_out_response
      ?W -> :copy_both_response
      ?D -> :data_row
      ?I -> :empty_query_response
      ?E -> :error_response
      ?V -> :function_call_response
      ?n -> :no_data
      ?N -> :notice_response
      ?A -> :notification_response
      ?t -> :parameter_description
      ?S -> :parameter_status
      ?1 -> :parse_complete
      ?s -> :portal_suspended
      ?Z -> :ready_for_query
      ?T -> :row_description
      _ -> :undefined
    end
  end

  def decode_payload(:authentication, payload) do
    case payload do
      <<0::integer-32>> ->
        :authentication_ok

      <<2::integer-32>> ->
        :authentication_kerberos_v5

      <<3::integer-32>> ->
        :authentication_cleartext_password

      <<5::integer-32, salt::binary-4>> ->
        {:authentication_md5_password, salt}

      <<6::integer-32>> ->
        :authentication_scm_credential

      <<7::integer-32>> ->
        :authentication_gss

      <<8::integer-32, rest::binary>> ->
        {:authentication_gss_continue, rest}

      <<9::integer-32>> ->
        :authentication_sspi

      _ ->
        :undefined
    end
  end

  def decode_payload(:parameter_status, payload) do
    case String.split(payload, <<0>>, trim: true) do
      [k, v] -> {k, v}
      _ -> :undefined
    end
  end

  def decode_payload(:backend_key_data, <<proc_id::integer-32, secret::integer-32>>) do
    %{procid: proc_id, secret: secret}
  end

  def decode_payload(:ready_for_query, payload) do
    case payload do
      <<"I">> -> :idle
      <<"T">> -> :transaction
      <<"E">> -> :error
    end
  end

  def decode_payload(:error_response, _payload) do
    :undefined
  end

  def decode_payload(_, _) do
    :undefined
  end
end

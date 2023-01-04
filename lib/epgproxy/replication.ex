defmodule Epgproxy.Replication do
  defmodule Decoder do
    @moduledoc """
    Functions for decoding different types of logical replication messages.
    """

    defmodule OidDatabase do
      @doc """
      Maps a numeric PostgreSQL type ID to a descriptive string.

      ## Examples

          iex> name_for_type_id(1700)
          "numeric"

          iex> name_for_type_id(25)
          "text"

          iex> name_for_type_id(3802)
          "jsonb"

      """
      def name_for_type_id(type_id) do
        case type_id do
          16 -> "bool"
          17 -> "bytea"
          18 -> "char"
          19 -> "name"
          20 -> "int8"
          21 -> "int2"
          22 -> "int2vector"
          23 -> "int4"
          24 -> "regproc"
          25 -> "text"
          26 -> "oid"
          27 -> "tid"
          28 -> "xid"
          29 -> "cid"
          30 -> "oidvector"
          114 -> "json"
          142 -> "xml"
          143 -> "_xml"
          194 -> "pg_node_tree"
          199 -> "_json"
          210 -> "smgr"
          600 -> "point"
          601 -> "lseg"
          602 -> "path"
          603 -> "box"
          604 -> "polygon"
          628 -> "line"
          629 -> "_line"
          650 -> "cidr"
          651 -> "_cidr"
          700 -> "float4"
          701 -> "float8"
          702 -> "abstime"
          703 -> "reltime"
          704 -> "tinterval"
          718 -> "circle"
          719 -> "_circle"
          774 -> "macaddr8"
          775 -> "_macaddr8"
          790 -> "money"
          791 -> "_money"
          829 -> "macaddr"
          869 -> "inet"
          1000 -> "_bool"
          1001 -> "_bytea"
          1002 -> "_char"
          1003 -> "_name"
          1005 -> "_int2"
          1006 -> "_int2vector"
          1007 -> "_int4"
          1008 -> "_regproc"
          1009 -> "_text"
          1010 -> "_tid"
          1011 -> "_xid"
          1012 -> "_cid"
          1013 -> "_oidvector"
          1014 -> "_bpchar"
          1015 -> "_varchar"
          1016 -> "_int8"
          1017 -> "_point"
          1018 -> "_lseg"
          1019 -> "_path"
          1020 -> "_box"
          1021 -> "_float4"
          1022 -> "_float8"
          1023 -> "_abstime"
          1024 -> "_reltime"
          1025 -> "_tinterval"
          1027 -> "_polygon"
          1028 -> "_oid"
          1033 -> "aclitem"
          1034 -> "_aclitem"
          1040 -> "_macaddr"
          1041 -> "_inet"
          1042 -> "bpchar"
          1043 -> "varchar"
          1082 -> "date"
          1083 -> "time"
          1114 -> "timestamp"
          1115 -> "_timestamp"
          1182 -> "_date"
          1183 -> "_time"
          1184 -> "timestamptz"
          1185 -> "_timestamptz"
          1186 -> "interval"
          1187 -> "_interval"
          1231 -> "_numeric"
          1263 -> "_cstring"
          1266 -> "timetz"
          1270 -> "_timetz"
          1560 -> "bit"
          1561 -> "_bit"
          1562 -> "varbit"
          1563 -> "_varbit"
          1700 -> "numeric"
          1790 -> "refcursor"
          2201 -> "_refcursor"
          2202 -> "regprocedure"
          2203 -> "regoper"
          2204 -> "regoperator"
          2205 -> "regclass"
          2206 -> "regtype"
          2207 -> "_regprocedure"
          2208 -> "_regoper"
          2209 -> "_regoperator"
          2210 -> "_regclass"
          2211 -> "_regtype"
          2949 -> "_txid_snapshot"
          2950 -> "uuid"
          2951 -> "_uuid"
          2970 -> "txid_snapshot"
          3220 -> "pg_lsn"
          3221 -> "_pg_lsn"
          3361 -> "pg_ndistinct"
          3402 -> "pg_dependencies"
          3614 -> "tsvector"
          3615 -> "tsquery"
          3642 -> "gtsvector"
          3643 -> "_tsvector"
          3644 -> "_gtsvector"
          3645 -> "_tsquery"
          3734 -> "regconfig"
          3735 -> "_regconfig"
          3769 -> "regdictionary"
          3770 -> "_regdictionary"
          3802 -> "jsonb"
          3807 -> "_jsonb"
          3905 -> "_int4range"
          3907 -> "_numrange"
          3909 -> "_tsrange"
          3911 -> "_tstzrange"
          3913 -> "_daterange"
          3927 -> "_int8range"
          4089 -> "regnamespace"
          4090 -> "_regnamespace"
          4096 -> "regrole"
          4097 -> "_regrole"
          _ -> type_id
        end
      end
    end

    defmodule Messages do
      @moduledoc """
      Different types of logical replication messages from Postgres
      """
      defmodule(Begin, do: defstruct([:final_lsn, :commit_timestamp, :xid]))
      defmodule(Commit, do: defstruct([:flags, :lsn, :end_lsn, :commit_timestamp]))
      defmodule(Origin, do: defstruct([:origin_commit_lsn, :name]))
      defmodule(Relation, do: defstruct([:id, :namespace, :name, :replica_identity, :columns]))
      defmodule(Insert, do: defstruct([:relation_id, :tuple_data]))

      defmodule(Update,
        do: defstruct([:relation_id, :changed_key_tuple_data, :old_tuple_data, :tuple_data])
      )

      defmodule(Delete,
        do: defstruct([:relation_id, :changed_key_tuple_data, :old_tuple_data])
      )

      defmodule(Truncate,
        do: defstruct([:number_of_relations, :options, :truncated_relations])
      )

      defmodule(Type,
        do: defstruct([:id, :namespace, :name])
      )

      defmodule(Unsupported, do: defstruct([:data]))

      defmodule(Relation.Column,
        do: defstruct([:flags, :name, :type, :type_modifier])
      )
    end

    require Logger

    @pg_epoch DateTime.from_iso8601("2000-01-01T00:00:00Z")

    alias Messages.{
      Begin,
      Commit,
      Origin,
      Relation,
      Relation.Column,
      Insert,
      Update,
      Delete,
      Truncate,
      Type,
      Unsupported
    }

    @doc """
    Parses logical replication messages from Postgres

    ## Examples

        iex> decode_message(<<73, 0, 0, 96, 0, 78, 0, 2, 116, 0, 0, 0, 3, 98, 97, 122, 116, 0, 0, 0, 3, 53, 54, 48>>)
        %Realtime.Adapters.Postgres.Decoder.Messages.Insert{relation_id: 24576, tuple_data: {"baz", "560"}}

    """
    def decode_message(message) when is_binary(message) do
      # Logger.debug("Message before conversion " <> message)
      decode_message_impl(message)
    end

    defp decode_message_impl(<<"B", lsn::binary-8, timestamp::integer-64, xid::integer-32>>) do
      %Begin{
        final_lsn: decode_lsn(lsn),
        commit_timestamp: pgtimestamp_to_timestamp(timestamp),
        xid: xid
      }
    end

    defp decode_message_impl(
           <<"C", _flags::binary-1, lsn::binary-8, end_lsn::binary-8, timestamp::integer-64>>
         ) do
      %Commit{
        flags: [],
        lsn: decode_lsn(lsn),
        end_lsn: decode_lsn(end_lsn),
        commit_timestamp: pgtimestamp_to_timestamp(timestamp)
      }
    end

    # TODO: Verify this is correct with real data from Postgres
    defp decode_message_impl(<<"O", lsn::binary-8, name::binary>>) do
      %Origin{
        origin_commit_lsn: decode_lsn(lsn),
        name: name
      }
    end

    defp decode_message_impl(<<"R", id::integer-32, rest::binary>>) do
      [
        namespace
        | [
            name
            | [<<replica_identity::binary-1, _number_of_columns::integer-16, columns::binary>>]
          ]
      ] = String.split(rest, <<0>>, parts: 3)

      # TODO: Handle case where pg_catalog is blank, we should still return the schema as pg_catalog
      friendly_replica_identity =
        case replica_identity do
          "d" -> :default
          "n" -> :nothing
          "f" -> :all_columns
          "i" -> :index
        end

      %Relation{
        id: id,
        namespace: namespace,
        name: name,
        replica_identity: friendly_replica_identity,
        columns: decode_columns(columns)
      }
    end

    defp decode_message_impl(
           <<"I", relation_id::integer-32, "N", number_of_columns::integer-16,
             tuple_data::binary>>
         ) do
      {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

      %Insert{
        relation_id: relation_id,
        tuple_data: decoded_tuple_data
      }
    end

    defp decode_message_impl(
           <<"U", relation_id::integer-32, "N", number_of_columns::integer-16,
             tuple_data::binary>>
         ) do
      {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

      %Update{
        relation_id: relation_id,
        tuple_data: decoded_tuple_data
      }
    end

    defp decode_message_impl(
           <<"U", relation_id::integer-32, key_or_old::binary-1, number_of_columns::integer-16,
             tuple_data::binary>>
         )
         when key_or_old == "O" or key_or_old == "K" do
      {<<"N", new_number_of_columns::integer-16, new_tuple_binary::binary>>,
       old_decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

      {<<>>, decoded_tuple_data} = decode_tuple_data(new_tuple_binary, new_number_of_columns)

      base_update_msg = %Update{
        relation_id: relation_id,
        tuple_data: decoded_tuple_data
      }

      case key_or_old do
        "K" -> Map.put(base_update_msg, :changed_key_tuple_data, old_decoded_tuple_data)
        "O" -> Map.put(base_update_msg, :old_tuple_data, old_decoded_tuple_data)
      end
    end

    defp decode_message_impl(
           <<"D", relation_id::integer-32, key_or_old::binary-1, number_of_columns::integer-16,
             tuple_data::binary>>
         )
         when key_or_old == "K" or key_or_old == "O" do
      {<<>>, decoded_tuple_data} = decode_tuple_data(tuple_data, number_of_columns)

      base_delete_msg = %Delete{
        relation_id: relation_id
      }

      case key_or_old do
        "K" -> Map.put(base_delete_msg, :changed_key_tuple_data, decoded_tuple_data)
        "O" -> Map.put(base_delete_msg, :old_tuple_data, decoded_tuple_data)
      end
    end

    defp decode_message_impl(
           <<"T", number_of_relations::integer-32, options::integer-8, column_ids::binary>>
         ) do
      truncated_relations =
        for relation_id_bin <- column_ids |> :binary.bin_to_list() |> Enum.chunk_every(4),
            do: relation_id_bin |> :binary.list_to_bin() |> :binary.decode_unsigned()

      decoded_options =
        case options do
          0 -> []
          1 -> [:cascade]
          2 -> [:restart_identity]
          3 -> [:cascade, :restart_identity]
        end

      %Truncate{
        number_of_relations: number_of_relations,
        options: decoded_options,
        truncated_relations: truncated_relations
      }
    end

    defp decode_message_impl(<<"Y", data_type_id::integer-32, namespace_and_name::binary>>) do
      [namespace, name_with_null] = :binary.split(namespace_and_name, <<0>>)
      name = String.slice(name_with_null, 0..-2)

      %Type{
        id: data_type_id,
        namespace: namespace,
        name: name
      }
    end

    defp decode_message_impl(binary), do: %Unsupported{data: binary}

    defp decode_tuple_data(binary, columns_remaining, accumulator \\ [])

    defp decode_tuple_data(remaining_binary, 0, accumulator) when is_binary(remaining_binary),
      do: {remaining_binary, accumulator |> Enum.reverse() |> List.to_tuple()}

    defp decode_tuple_data(<<"n", rest::binary>>, columns_remaining, accumulator),
      do: decode_tuple_data(rest, columns_remaining - 1, [nil | accumulator])

    defp decode_tuple_data(<<"u", rest::binary>>, columns_remaining, accumulator),
      do: decode_tuple_data(rest, columns_remaining - 1, [:unchanged_toast | accumulator])

    defp decode_tuple_data(
           <<"t", column_length::integer-32, rest::binary>>,
           columns_remaining,
           accumulator
         ),
         do:
           decode_tuple_data(
             :erlang.binary_part(rest, {byte_size(rest), -(byte_size(rest) - column_length)}),
             columns_remaining - 1,
             [
               :erlang.binary_part(rest, {0, column_length}) | accumulator
             ]
           )

    defp decode_columns(binary, accumulator \\ [])
    defp decode_columns(<<>>, accumulator), do: Enum.reverse(accumulator)

    defp decode_columns(<<flags::integer-8, rest::binary>>, accumulator) do
      [name | [<<data_type_id::integer-32, type_modifier::integer-32, columns::binary>>]] =
        String.split(rest, <<0>>, parts: 2)

      decoded_flags =
        case flags do
          1 -> [:key]
          _ -> []
        end

      decode_columns(columns, [
        %Column{
          name: name,
          flags: decoded_flags,
          type: OidDatabase.name_for_type_id(data_type_id),
          # type: data_type_id,
          type_modifier: type_modifier
        }
        | accumulator
      ])
    end

    defp pgtimestamp_to_timestamp(microsecond_offset) when is_integer(microsecond_offset) do
      {:ok, epoch, 0} = @pg_epoch

      DateTime.add(epoch, microsecond_offset, :microsecond)
    end

    defp decode_lsn(<<xlog_file::integer-32, xlog_offset::integer-32>>),
      do: {xlog_file, xlog_offset}
  end

  require Logger

  use Postgrex.ReplicationConnection
  require Logger

  alias Decoder.Messages.{
    Begin,
    Relation,
    Insert,
    Update,
    Delete,
    Commit
  }

  def start_link(args) do
    host =
      Application.get_env(:epgproxy, :db_host)
      |> String.to_charlist()

    opts = [
      host: host,
      port: Application.get_env(:epgproxy, :db_port),
      username: Application.get_env(:epgproxy, :db_user),
      database: Application.get_env(:epgproxy, :db_name),
      password: Application.get_env(:epgproxy, :db_password)
    ]

    init = %{
      publication: "epgproxy_publication",
      slot_name: "epgproxy_slot"
    }

    Postgrex.ReplicationConnection.start_link(__MODULE__, init, opts)
  end

  @spec stop(pid) :: :ok
  def stop(pid) do
    GenServer.stop(pid)
  end

  @impl true
  def init(args) do
    tid = :ets.new(__MODULE__, [:public, :set])
    state = %{tid: tid, step: nil, ts: nil}
    {:ok, Map.merge(args, state)}
  end

  @impl true
  def handle_connect(state) do
    query =
      "CREATE_REPLICATION_SLOT #{state.slot_name} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"

    {:query, query, %{state | step: :create_slot}}
  end

  @impl true
  def handle_result(results, %{step: :create_slot} = state) when is_list(results) do
    query =
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL 0/0 (proto_version '1', publication_names '#{state.publication}')"

    {:stream, query, [], %{state | step: :streaming}}
  end

  def handle_result(_results, state) do
    {:noreply, state}
  end

  @impl true
  def handle_data(<<?w, _header::192, msg::binary>>, state) do
    new_state =
      Decoder.decode_message(msg)
      |> process_message(state)

    {:noreply, new_state}
  end

  # keepalive
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
    messages =
      case reply do
        1 -> [<<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>]
        0 -> []
      end

    {:noreply, messages, state}
  end

  def handle_data(data, state) do
    Logger.error("Unknown data: #{inspect(data)}")
    {:noreply, state}
  end

  defp process_message(%Relation{id: id, columns: columns, namespace: schema, name: table}, state) do
    :ets.insert(state.tid, {id, columns, schema, table})
    state
  end

  defp process_message(%Begin{commit_timestamp: ts}, state) do
    %{state | ts: ts}
  end

  defp process_message(%Commit{}, state) do
    %{state | ts: nil}
  end

  defp process_message(%Insert{relation_id: id} = msg, state) do
    state
  end

  defp process_message(%Update{relation_id: id} = msg, state) do
    table_by_id(state.tid, id)
    |> Epgproxy.Cache.drop_for_table()

    state
  end

  defp process_message(%Delete{relation_id: id} = msg, state) do
    Logger.debug("Got message: #{inspect(msg)}")
    state
  end

  defp process_message(msg, state) do
    Logger.error("Unknown message: #{inspect(msg)}")
    state
  end

  def table_by_id(tid, id) do
    :ets.lookup_element(tid, id, 4)
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time(), do: System.os_time(:microsecond) - @epoch
end

defmodule Epgproxy.Cache do
  require Logger

  @cache_table __MODULE__

  def init_table() do
    :ets.new(@cache_table, [:named_table, :public])
  end

  def cached?(query) do
    {:ok, parsed_query} = EpgQuery.parse(query)

    case :ets.lookup(@cache_table, key(parsed_query)) do
      [_ | _] ->
        true

      _ ->
        false
    end
  end

  def get(query) do
    {:ok, record} = EpgQuery.parse(query)

    case :ets.lookup(@cache_table, key(record)) do
      [{_, value, _, _}] ->
        {:ok, value}

      _ = r ->
        {:error, :not_found}
    end
  end

  def handle_cache(query, result) do
    {:ok, record} = EpgQuery.parse(query)

    tables = list_tables(record)
    :ets.insert(@cache_table, {key(record), result, record, tables})
  end

  def handle_query(query) do
    {:ok, parsed_query} = EpgQuery.parse(query)

    case :ets.lookup(@cache_table, key(parsed_query)) do
      [] ->
        put(parsed_query)

      _ ->
        Logger.debug("Cached query: #{inspect(query)}")
    end
  end

  def key(record) do
    :erlang.phash2(record, 1000)
  end

  def put(record) do
    # TODO: use query's fingerprint as key
    tables = list_tables(record)
    :ets.insert(@cache_table, {key(record), record, tables})
  end

  def list_tables(parsed_query) do
    parsed_query["stmts"]
    |> Enum.reduce(MapSet.new(), fn stmt, acc ->
      from = stmt["stmt"]["SelectStmt"]["fromClause"]

      if from != nil do
        Enum.reduce(from, acc, fn from, acc1 ->
          analize_from(from, MapSet.new())
          |> MapSet.union(acc1)
        end)
        |> MapSet.to_list()
      else
        nil
      end

      # acc
    end)
  end

  def analize_from(%{"JoinExpr" => join}, acc) do
    analize_join(join, acc)
  end

  def analize_from(_, acc) do
    acc
  end

  def analize_join(%{"jointype" => _type, "larg" => larg, "quals" => _, "rarg" => rarg}, acc) do
    join_larg(larg, acc)
    |> MapSet.union(join_rarg(rarg, acc))
  end

  def join_rarg(%{"RangeVar" => %{"relname" => rel}}, acc) do
    MapSet.put(acc, rel)
  end

  def join_larg(%{"RangeVar" => %{"relname" => rel}}, acc) do
    MapSet.put(acc, rel)
  end

  def join_larg(nested, acc) do
    analize_from(nested, acc)
  end
end

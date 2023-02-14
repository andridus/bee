defmodule Bee.Api do
  @moduledoc false
  defmacro __using__(_) do
    quote do
      import Ecto.Query
      import Bee.Api

      def schema, do: @schema

      def changeset(changeset, params, type \\ "create") do
        apply(schema(), :"changeset_#{type}", [changeset, params])
      end

      def json_fields(append) do
        schema().__json__() ++ append
      end

      def get_by(params \\ [where: [], order: [asc: :inserted_at]]) do
        get_by!(params)
        |> case do
          nil -> {:error, :not_found}
          data -> {:ok, data}
        end
      end

      def get_by!(params \\ [where: [], order: [asc: :inserted_at]]) do
        params
        |> default_params()
        |> repo().one()
      end

      def get(_id, _params \\ [where: [], order: [asc: :inserted_at]])
      def get(nil, _params), do: {:error, :id_is_nil}

      def get(id, params) do
        get!(id, params)
        |> case do
          nil -> {:error, :not_found}
          data -> {:ok, data}
        end
      end

      def get!(id, params \\ [where: [], order: [asc: :inserted_at]]) do
        params
        |> default_params()
        |> repo().get(id)
      end

      def blank(), do: struct(schema())

      def all(params \\ [where: [], order: []]) do
        params
        |> default_params()
        |> repo().all()
      end

      @doc """
        insert_many

        > insert only array of map (not a struct)

        options:
        [
          conflict_target: :column_name | {:unsafe_fragment, binary_fragment}
          on_conflict: :atom | :tuple (one of :raise, :nothing, :replace_all, {:replace_all_except, fields}, {:replace, fields} )
          batch: integer (with the number about insert records each step - for many rows)
        ]
      """
      def insert_many(list, options \\ []) do
        batch = options[:batch]
        options = options |> Keyword.drop([:batch])
        total = Enum.count(list)
        if is_nil(batch) do
          {num, _} = repo().insert_all(schema(), list, options)
          {:ok, %{total: total, inserted: num, conflicts: total - num}}
        else
          num =
            list
            |> Enum.chunk_every(batch)
            |> Enum.map(& repo().insert_all(schema(), &1, options))
            |> Enum.reduce(0, fn {num,_}, acc -> acc + num end)

          {:ok, %{total: total, inserted: num, conflicts: total - num}}
        end
      end
      def insert(%Ecto.Changeset{} = model),
        do: model |> repo().insert()

      def insert(params) do
        schema()
        |> struct()
        |> schema().changeset_insert(params)
        |> repo().insert()
      end

      def exists?(id) when is_bitstring(id),
        do: exists?(where: [id: id])

      def exists?(params) do
        params
        |> default_params()
        |> repo().exists?()
      end

      def exists(params) do
        if exists?(params) do
          {:ok, true}
        else
          {:error, false}
        end
      end

      def update(%Ecto.Changeset{} = model),
        do: model |> repo().update()

      def update(%{"id" => id} = model) do
        params = Map.drop(model, ["id"])
        __update_model__(id, params)
      end

      def update(%{id: id} = model) do
        params = Map.drop(model, [:id])
        __update_model__(id, params)
      end

      def update(id, params) when is_bitstring(id), do: __update_model__(id, params)
      def update(%{id: id}, params) when is_bitstring(id), do: __update_model__(id, params)
      def update(_invalid_model, _params), do: {:error, :invalid_model}

      defp __update_model__(id, params) do
        id
        |> get!()
        |> schema().changeset_update(params)
        |> repo().update()
      end

      def prepare_to_delete(model, opts \\ []) do
        schema().changeset_delete(model)
      rescue
        _ ->
          if is_nil(opts[:error_silent]) do
            raise "The function `changeset_delete` is not defined for `#{schema()}`."
          else
            model
          end
      end
      def delete_many_by_id(ids, options \\ []) do
        batch = options[:batch]
        options = options |> Keyword.drop([:batch])
        total = Enum.count(ids)
        if is_nil(batch) do
          {num, _} =
            [where: [{{:in, :id}, ids}]]
            |> default_params()
            |> repo().delete_all(options)
          {:ok, %{total: total, deleted: num}}
        else
          num =
            ids
            |> Enum.chunk_every(batch)
            |> Enum.map(fn ids1 ->
              [where: [{{:in, :id}, ids1}]]
              |> default_params()
              |> repo().delete_all(options)
            end)
            |> Enum.reduce(0, fn {num,_}, acc -> acc + num end)

          {:ok, %{total: total, inserted: num}}
        end
      end
      def delete(id) when is_bitstring(id),
        do: id |> get!() |> delete()

      def delete(model), do: model |> repo().delete()

      def default_params(params, sc \\ nil) do
        schm_ = sc || schema()
        sch = from(schm_)

        params
        |> Enum.reduce(sch, fn
          {:where, params}, sch ->
            sch |> where(^default_conditions(params))

          {:or_where, params}, sch ->
            sch |> or_where(^default_conditions(params))

          {:order, params}, sch ->
            sch |> order_by(^params)

          {:preload, params}, sch ->
            lst =
              Enum.reduce(params, [], fn
                {k, v}, acc ->
                  {_k, _t, opts} = schm_.__live_fields__() |> List.keyfind!(k, 0)
                  [{k, default_params(v, opts[:schema])} | acc]

                k, acc when is_atom(k) ->
                  [k | acc]
              end)
              |> Enum.reverse()

            sch |> preload(^lst)

          {:select, params}, sch ->
            sch |> select([c], map(c, ^params))

          {:group, params}, sch ->
            sch |> group_by(^params)

          {:distinct, params}, sch ->
            sch |> distinct(^params)

          {:limit, params}, sch ->
            sch |> limit(^params)

          {:offset, params}, sch ->
            sch |> offset(^params)

          {:inspect, true}, sch ->
            IO.inspect(sch)

          _, sch ->
            sch
        end)
      end

      defp default_conditions(params) do
        Enum.reduce(params, nil, fn
          {{:lt, key}, value}, conditions ->
            if is_nil(conditions) do
              dynamic([p], field(p, ^key) < ^value)
            else
              dynamic([p], field(p, ^key) < ^value and ^conditions)
            end

          {{:elt, key}, value}, conditions ->
            if is_nil(conditions) do
              dynamic([p], field(p, ^key) <= ^value)
            else
              dynamic([p], field(p, ^key) <= ^value and ^conditions)
            end

          {{:gt, key}, value}, conditions ->
            if is_nil(conditions) do
              dynamic([p], field(p, ^key) >= ^value)
            else
              dynamic([p], field(p, ^key) >= ^value and ^conditions)
            end

          {{:egt, key}, value}, conditions ->
            if is_nil(conditions) do
              dynamic([p], field(p, ^key) > ^value)
            else
              dynamic([p], field(p, ^key) > ^value and ^conditions)
            end

          {{:ilike, key}, value}, conditions ->
            value = "%#{value}%"

            if is_nil(conditions) do
              dynamic([p], ilike(field(p, ^key), ^value))
            else
              dynamic([p], ilike(field(p, ^key), ^value) and ^conditions)
            end

          {{:in, key}, value}, conditions ->
            if is_nil(conditions) do
              dynamic([p], field(p, ^key) in ^value)
            else
              dynamic([p], field(p, ^key) in ^value and ^conditions)
            end

          # {{:ago, key}, {value, opt}}, conditions ->
          #   if is_nil(conditions) do
          #     dynamic([p], ago(field(p, ^key), ^value, ^opt))
          #   else
          #     dynamic([p], ago(field(p, ^key), ^value, ^opt) and ^conditions)
          #   end

          # {{:date_add, key}, {value, opt}}, conditions ->
          #     if is_nil(conditions) do
          #       dynamic([p], date_add(field(p, ^key), ^value, ^opt))
          #     else
          #       dynamic([p], date_add(field(p, ^key), ^value, ^opt) and ^conditions)
          #     end
          # {{:datetime_add, key}, {value, opt}}, conditions ->
          #   if is_nil(conditions) do
          #     dynamic([p], datetime_add(field(p, ^key), ^value, ^opt))
          #   else
          #     dynamic([p], datetime_add(field(p, ^key), ^value, ^opt) and ^conditions)
          #   end
          {:not_nil, key}, conditions ->
            if is_nil(conditions) do
              dynamic([p], not is_nil(field(p, ^key)))
            else
              dynamic([p], not is_nil(field(p, ^key)) and ^conditions)
            end

          {key, nil}, conditions ->
            if is_nil(conditions) do
              dynamic([p], is_nil(field(p, ^key)))
            else
              dynamic([p], is_nil(field(p, ^key)) and ^conditions)
            end

          {key, value}, conditions ->
            if is_nil(conditions) do
              dynamic([p], field(p, ^key) == ^value)
            else
              dynamic([p], field(p, ^key) == ^value and ^conditions)
            end
        end)
        |> Kernel.||([])
      end

      def count(params \\ []) do
        params
        |> default_params()
        |> repo().aggregate(:count, :id)
      end

      @doc """
        get json data
      """
      def json(_model, _include \\ [])
      def json(nil, _), do: nil

      def json(model, include) do
        model
        |> preload_json(include)
        |> Map.take(json_fields(include))
      end

      def insert_or_update(%Ecto.Changeset{action: :insert} = model), do: insert(model)

      def insert_or_update(%Ecto.Changeset{action: :update, data: %{id: _id}} = model),
        do: update(model)

      def insert_or_update(%{id: id} = model) when not is_nil(id), do: update(model)
      def insert_or_update(model), do: insert(model)

      defoverridable changeset: 2,
                     changeset: 3,
                     json_fields: 1,
                     get: 1,
                     get: 2,
                     get_by: 1,
                     all: 1,
                     insert: 1,
                     update: 1,
                     delete: 1,
                     json: 2,
                     count: 1
    end
  end

  def repo, do: Application.get_env(:bee, :repo)

  def preload_json(model, include \\ []) do
    model
    |> repo().preload(include)
    |> Map.take(include)
    |> Map.to_list()
    |> Enum.map(fn {key, value} ->
      module = get_module(model, key, include)

      if is_list(value) do
        {key, Enum.map(value, &(apply(module, :json, [&1]) |> unwrap()))}
      else
        {key, apply(module, :json, [value]) |> unwrap()}
      end
    end)
    |> Map.new()
    |> then(&Map.merge(model, &1))
  end

  def get_module(model, key, _includes) do
    Ecto.build_assoc(model, key, %{})
    |> Map.get(:__struct__)
    |> to_string()
    |> String.replace("Schema", "Api")
    |> atomize()
  end

  def atomize(string) when is_bitstring(string) do
    string |> String.to_existing_atom()
  rescue
    ArgumentError -> String.to_atom(string)
  end

  def unwrap({:ok, value}), do: value
  def unwrap(err), do: err
  def unwrap!({_, value}), do: value

  defoverridable repo: 0
end

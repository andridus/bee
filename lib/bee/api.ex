defmodule Bee.Api do
  @moduledoc """
    Generate an Api for  repository.
  """
  defmacro __using__(args) do
    quote do
      import Ecto.Query
      import Bee.Api

      def schema, do: @schema
      def blank(), do: struct(schema())

      def changeset(changeset, params, type \\ "insert") do
        apply(schema(), :"changeset_#{type}", [changeset, params])
      end

      def json(entity) do
        Map.take(entity, schema().bee_json())
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

      def all(params \\ [where: [], order: []]) do
        params
        |> default_params()
        |> repo().all()
      end

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
            |> Enum.map(&repo().insert_all(schema(), &1, options))
            |> Enum.reduce(0, fn {num, _}, acc -> acc + num end)

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

      def update(id, params) when is_bitstring(id) or is_integer(id),
        do: __update_model__(id, params)

      def update(%{id: id}, params) when is_bitstring(id) or is_integer(id),
        do: __update_model__(id, params)

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
            |> Enum.reduce(0, fn {num, _}, acc -> acc + num end)

          {:ok, %{total: total, inserted: num}}
        end
      end

      def delete(id) when is_bitstring(id) or is_integer(id),
        do: id |> get!() |> delete()

      def delete(%Ecto.Changeset{} = changeset), do: changeset |> repo().delete()
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
                  relations = schm_.bee_relation_fields()

                  if k in relations do
                    {_k, opts} = schm_.bee_raw_fields() |> List.keyfind!(k, 0)
                    [{k, default_params(v, opts[:type])} | acc]
                  else
                    raise "The '#{k}' field not exist in relation fields of '#{schm_}' only '#{relations}"
                  end

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

          {:debug, true}, sch ->
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

      def count(params \\ []) do
        aggregate(:count, :id, params)
      end

      def aggregate(mode, field, params \\ []) do
        params
        |> default_params()
        |> repo().aggregate(mode, field)
      end

      def insert_or_update(%Ecto.Changeset{action: :insert} = model), do: insert(model)

      def insert_or_update(%Ecto.Changeset{action: :update, data: %{id: _id}} = model),
        do: update(model)

      def insert_or_update(%{id: id} = model) when not is_nil(id), do: update(model)
      def insert_or_update(model), do: insert(model)

      def repo, do: unquote(args)[:repo] || Application.get_env(:bee, :repo)

      defoverridable changeset: 2,
                     changeset: 3,
                     json: 1,
                     get: 1,
                     get: 2,
                     get_by: 1,
                     all: 1,
                     insert: 1,
                     update: 1,
                     delete: 1,
                     count: 1
    end
  end

  @type option ::
          {:where, keyword()}
          | {:or_where, keyword()}
          | {:preload, list(atom()) | keyword()}
          | {:order, list(atom()) | keyword()}
          | {:select, list(atom()) | list(String.t())}
          | {:group, list(atom())}
          | {:distinct, list(atom())}
          | {:debug, boolean()}
          | {:limit, integer()}
          | {:offset, integer()}
  @type id :: binary()
  @type options :: [option]

  @doc """
  Obtain a single model from the data store where the primary key matches the
  given id.
  Returns `{:error, :not_found}` if no result was found. If the struct in the queryable
  has no or more than one primary key, it will raise an argument error.

  ### Options
    * `:preload` - Preload relations on query
    * `:order` - Set order for the query
    * `:select` - Fields should be returned by query
    * `:group` - Grouping query by fields
    * `:distinct` - List of columns (atoms) that remove duplicate on query
    * `:debug` - Show in console, the generated Ecto query before call
    * `:limit` - Set the limit for the query
    * `:offset` - Set the offset for the query

  See the ["Knowing Options"](#module-options) section at the module
  documentation for more details.
  ## Example
      Post.Api.get(42)
      Post.Api.get(42, preload: [:comments])
      Post.Api.get(42, preload: [comments: [preload: [:users]]])
  """
  @doc group: "Query API"
  @callback get(id, options) :: {:ok, Ecto.Schema.t()} | {:ok, term} | {:error, :not_found}

  @doc """
  Similar to `get/2` but raises `Ecto.NoResultsError` if no record was found.

  ### Options
    * `:preload` - Preload relations on query
    * `:order` - Set order for the query
    * `:select` - Fields should be returned by query
    * `:group` - Grouping query by fields
    * `:distinct` - List of columns (atoms) that remove duplicate on query
    * `:debug` - Show in console, the generated Ecto query before call
    * `:limit` - Set the limit for the query
    * `:offset` - Set the offset for the query

  See the ["Knowing Options"](#module-options) section at the module
  documentation for more details.
  ## Example
      Post.Api.get!(42)
      Post.Api.get!(42, preload: [:comments])
      Post.Api.get!(42, preload: [comments: [preload: [:users]]])
  """
  @doc group: "Query API"
  @callback get!(id, options) :: {:ok, Ecto.Schema.t()} | {:ok, term} | {:error, :not_found}

  @doc """
  Obtain a single model from the data store where the conditions on `where` or `or_where` matches.
  Returns `{:error, :not_found}` if no result was found. If the struct in the queryable
  has no or more than one primary key, it will raise an argument error.

  ### Options
    * `:where` - The group conditions for WHERE argument in SQL that results in AND for any keys.
    * `:or_where` - Using after `:where`, grouping conditions in a OR for WHERE argument in SQL.
    * `:preload` - Preload relations on query
    * `:order` - Set order for the query
    * `:select` - Fields should be returned by query
    * `:group` - Grouping query by fields
    * `:distinct` - List of columns (atoms) that remove duplicate on query
    * `:debug` - Show in console, the generated Ecto query before call
    * `:limit` - Set the limit for the query
    * `:offset` - Set the offset for the query

  See the ["Knowing Options"](#module-options) section at the module
  documentation for more details.
  ## Example
      Post.Api.get_by(where: [title: "My Post"])
      Post.Api.get_by(where: [title: "My Post"], preload: [:comments])
      Post.Api.get_by(where: [title: "My Post"], preload: [comments: [preload: [:users]]])
  """
  @doc group: "Query API"
  @callback get_by(options) :: {:ok, Ecto.Schema.t()} | {:ok, term} | {:error, :not_found}

  @doc """
  Similar to `get_by/2` but raises `Ecto.NoResultsError` if no record was found.

  ### Options
    * `:where` - The group conditions for WHERE argument in SQL that results in AND for any keys.
    * `:or_where` - Using after `:where`, grouping conditions in a OR for WHERE argument in SQL.
    * `:preload` - Preload relations on query
    * `:order` - Set order for the query
    * `:select` - Fields should be returned by query
    * `:group` - Grouping query by fields
    * `:distinct` - List of columns (atoms) that remove duplicate on query
    * `:debug` - Show in console, the generated Ecto query before call
    * `:limit` - Set the limit for the query
    * `:offset` - Set the offset for the query

  See the ["Knowing Options"](#module-options) section at the module
  documentation for more details.
  ## Example
      Post.Api.get_by!(where: [title: "My Post"], or_where: [title: "My Post 2"])
      Post.Api.get_by!(where: [title: "My Post"], preload: [:comments])
      Post.Api.get_by!(where: [title: "My Post"], preload: [comments: [preload: [:users]]])
  """
  @doc group: "Query API"
  @callback get_by!(options) :: {:ok, Ecto.Schema.t()} | {:ok, term} | {:error, :not_found}

  @doc """
  Obtain all models from the data store where the conditions on `where` or `or_where` matches.
  Returns `[]` (empty list) if no result was found.

  ### Options
    * `:where` - The group conditions for WHERE argument in SQL that results in AND for any keys.
    * `:or_where` - Using after `:where`, grouping conditions in a OR for WHERE argument in SQL.
    * `:preload` - Preload relations on query
    * `:order` - Set order for the query
    * `:select` - Fields should be returned by query
    * `:group` - Grouping query by fields
    * `:distinct` - List of columns (atoms) that remove duplicate on query
    * `:debug` - Show in console, the generated Ecto query before call
    * `:limit` - Set the limit for the query
    * `:offset` - Set the offset for the query

  See the ["Knowing Options"](#module-options) section at the module
  documentation for more details.
  ## Example
      Post.Api.all(where: [area: "Financial"])
      Post.Api.all(where: [area: "Financial"], preload: [:comments])
      Post.Api.all(where: [area: "Financial"], preload: [comments: [preload: [:users]]])
  """
  @doc group: "Query API"
  @callback all(options) :: list(Ecto.Schema.t()) | list(term) | []

  @doc """
  Inserts an entity on data store by struct defined `Ecto.Schema`,
  or map of data, or a changeset.

  In case a struct or map is given, the struct or map is converted
  into a changeset with all non-nil fields as part of the changeset.

  In case a changeset is given, the changes in the changeset are merged
  with the struct fields, and all of them are sent to the
  database.

  ## Example
      Post.Api.insert(%Post{title: "My Post})
      Post.Api.insert(%{title: "My Post})
      Post.Api.insert(%Ecto.Changeset{changes: %{title: "My Post}})
  """
  @doc group: "Query API"
  @callback insert(params :: map()) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  @callback insert(struct :: Ecto.Changeset.t()) ::
              {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}

  @doc """
  Inserts many of an entity on data store by struct defined `Ecto.Schema`,
  or map of data, or a changeset.

  ### Options
    * `:conflict_target` - :column_name | {:unsafe_fragment, binary_fragment}
    * `:on_conflict` - one of :raise, :nothing, :replace_all, {:replace_all_except, fields}, {:replace, fields}
    * `:batch` - quantity integer of items by batch.


  ## Example
      Post.Api.insert_many(
        [
        %{id: 1, title: "My Post"},
        %{id: 2, title: "My Post 2"},
        ...
        ],
        [
          on_conflict: {:replace_all_except, [:id]},
          conflict_target: [:id],
          batch: 10
        ]
      )
  """
  @doc group: "Query API"
  @callback insert_many(data :: list(map()), opts :: keyword()) ::
              {:ok, %{total: integer(), inserted: integer(), conflicts: integer()}}
              | {:error, term()}

  @doc """
  Updates an entity using its primary key on `id` in a given map or struct, or a given changeset.

  *** Only valid when `id` is a primary key***

  ## Example
      post = %Post{id: 1, title: "My Post}

      Post.Api.update(post, %{title: "My Post2})
      Post.Api.update(%{id: 1 title: "My Post2})
      Post.Api.update(%Ecto.Changeset{})
  """
  @doc group: "Query API"
  @callback update(params :: map()) :: {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  @callback update(struct :: Ecto.Schema.t() | Ecto.Changeset.t()) ::
              {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  @callback update(struct :: Ecto.Schema.t(), params :: map()) ::
              {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}

  @doc """
  Delete many of an entity on data store by list of id (primary key).

  ### Options
    * `:batch` - quantity integer of items by batch.


  ## Example
      Post.Api.delete_many_by_id([1,2,3,4,5], [ batch: 10 ])
  """
  @doc group: "Query API"
  @callback delete_many_by_id(ids :: list(binary()) | list(integer()), opts :: keyword()) ::
              {:ok, %{total: integer(), deleted: integer()}} | {:error, term()}

  @doc """
  Delete an entity using its primary key on `id` in a given entity or entity or changeset.

  ## Example
      Post.Api.delete(1)
      Post.Api.delete(%Post{id: 1})
      Post.Api.delete(%Ecto.Changeset{})
  """
  @doc group: "Query API"
  @callback delete(id :: String.t() | integer()) ::
              {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, :not_found}
  @callback delete(model :: Ecto.Schema.t()) ::
              {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, :not_found}
  @callback delete(changeset :: Ecto.Changeset.t()) ::
              {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()} | {:error, :not_found}

  @doc """
  Check if exists an entity, return the boolean true or false .

  ### Options
    * `:where` - The group conditions for WHERE argument in SQL that results in AND for any keys.
    * `:or_where` - Using after `:where`, grouping conditions in a OR for WHERE argument in SQL.
    * `:group` - Grouping query by fields
    * `:distinct` - List of columns (atoms) that remove duplicate on query
    * `:debug` - Show in console, the generated Ecto query before call
    * `:limit` - Set the limit for the query
    * `:offset` - Set the offset for the query

  ## Example
      Post.Api.exists?(where: [title: "My Post"])
  """
  @doc group: "Query API"
  @callback exists?(options) :: true | false

  @doc """
  Check if exists an entity. Return the tuple {:ok, true} | {:error, false}

  ### Options
    * `:where` - The group conditions for WHERE argument in SQL that results in AND for any keys.
    * `:or_where` - Using after `:where`, grouping conditions in a OR for WHERE argument in SQL.
    * `:group` - Grouping query by fields
    * `:distinct` - List of columns (atoms) that remove duplicate on query
    * `:debug` - Show in console, the generated Ecto query before call
    * `:limit` - Set the limit for the query
    * `:offset` - Set the offset for the query

  ## Example
      Post.Api.exists(1)
      Post.Api.exists(where: [title: "My Post"])
  """
  @doc group: "Query API"
  @callback exists(id :: bitstring()) :: {:ok, true} | {:error, false}
  @callback exists(options) :: {:ok, true} | {:error, false}

  @doc """
  Count an entity. Return the integer with the value

  ### Options
    * `:where` - The group conditions for WHERE argument in SQL that results in AND for any keys.
    * `:or_where` - Using after `:where`, grouping conditions in a OR for WHERE argument in SQL.
    * `:group` - Grouping query by fields
    * `:distinct` - List of columns (atoms) that remove duplicate on query
    * `:debug` - Show in console, the generated Ecto query before call
    * `:limit` - Set the limit for the query
    * `:offset` - Set the offset for the query

  ## Example
      Post.Api.count(where: [title: "My Post"])
  """
  @doc group: "Query API"
  @callback count(options) :: integer()

  @doc """
  If the query has a limit, offset, distinct or combination set, it will be
  automatically wrapped in a subquery in order to return the
  proper result.

  The aggregation will fail if any `group_by` field is set.

  Calculate the given `aggregate`.

  The `mode` is one of :avg | :count | :max | :min | :sum
  ### Options
    * `:where` - The group conditions for WHERE argument in SQL that results in AND for any keys.
    * `:or_where` - Using after `:where`, grouping conditions in a OR for WHERE argument in SQL.
    * `:group` - Grouping query by fields
    * `:distinct` - List of columns (atoms) that remove duplicate on query
    * `:debug` - Show in console, the generated Ecto query before call
    * `:limit` - Set the limit for the query
    * `:offset` - Set the offset for the query
    * `:aggregate` - Settings of aggregate data

  ## Example
      Post.Api.aggregate(:count, :id, where: [title: "My Post"])
  """
  @doc group: "Query API"
  @callback aggregate(mode :: :avg | :count | :max | :min | :sum, field :: atom(), options) ::
              integer()
end

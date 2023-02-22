defmodule Bee.Api do
  import Ecto.Query

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
        |> default_params(schema())
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
        |> default_params(schema())
        |> repo().get(id)
      end

      def all(params \\ [where: [], order: []]) do
        params
        |> default_params(schema())
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
            reraise "The function `changeset_delete` is not defined for `#{schema()}`.",
                    __STACKTRACE__
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
            [where: [id: {:in, ids}]]
            |> default_params(schema())
            |> repo().delete_all(options)

          {:ok, %{total: total, deleted: num}}
        else
          num =
            ids
            |> Enum.chunk_every(batch)
            |> Enum.map(fn ids1 ->
              [where: [id: {:in, ids1}]]
              |> default_params(schema())
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

      def exists?(id) when is_bitstring(id),
        do: exists?(where: [id: id])

      def exists?(params) do
        params
        |> default_params(schema())
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
        |> default_params(schema())
        |> repo().aggregate(mode, field)
      end

      def insert_or_update(%Ecto.Changeset{action: :insert} = model), do: insert(model)

      def insert_or_update(%Ecto.Changeset{action: :update, data: %{id: _id}} = model),
        do: update(model)

      def insert_or_update(%{id: id} = model) when not is_nil(id), do: update(model)
      def insert_or_update(model), do: insert(model)

      def repo, do: unquote(args)[:repo] || Application.get_env(:bee, :repo)

      def params_to_query(params) do
        limit = params[:limit]
        offset = params[:offset]

        permission = params[:permission]
        fields = params[:fields]
        assocs = params[:assocs]
        filter = params[:filter]

        {fields, assocs}
        |> parse_fields(schema(), permission)
        |> parse_filter(filter)
        |> maybe_add(:offset, offset)
        |> maybe_add(:limit, limit)
      end

      defp parse_filter(query, nil), do: query

      defp parse_filter(query, _filter) do
        Keyword.put(query, :where, [])
      end

      defp parse_fields({nil, nil}, _module, _permission), do: []

      defp parse_fields({fields, _assocs}, module, permission) when is_bitstring(fields) do
        fields = for str <- String.split(fields, ","), do: String.trim(str)
        normalize_fields(fields, module, permission)
      end

      defp parse_fields({nil, assocs}, module, permission) when is_bitstring(assocs) do
        fields = for str <- String.split(assocs, ","), do: String.trim(str)
        normalize_assocs(fields, module, permission)
      end

      defp parse_fields(_, _module, _permission), do: []

      defp normalize_fields(fields, module, permission) do
        exposed = for atom <- module.bee_permission(permission), do: to_string(atom)

        assoc_fields =
          for flds <- fields, String.contains?(flds, "."), do: String.split(flds, ".")

        simple_fields =
          for flds <- fields,
              !String.contains?(flds, "."),
              flds in exposed,
              do: String.to_existing_atom(flds)

        assoc_fields_str =
          for {atom, module} <- module.bee_relation_raw_fields(), do: {to_string(atom), module}

        parsed = assoc_fields |> Enum.reduce(%{}, &parse_fields_recv(&1, &2))

        preload =
          for {key, value} <- parsed do
            List.keyfind(assoc_fields_str, key, 0)
            |> case do
              {_, {module, _}} ->
                {String.to_existing_atom(key), normalize_fields(value["_"], module, permission)}

              _ ->
                []
            end
          end

        if length(preload) > 0 do
          [{:preload, preload}]
        else
          []
        end
        |> Keyword.put(:select, simple_fields)
      end

      defp normalize_assocs(fields, module, permission) do
        assoc_modules =
          for {f, :relation_type, opt} <- module.__live_fields__, do: {f, opt[:schema]}

        assoc_fields = Enum.map(fields, &String.split(&1, "."))
        assoc_fields_str = for atom <- module.__assoc_fields__, do: to_string(atom)

        preload =
          recursive_assoc(assoc_fields, assoc_fields_str, assoc_modules, permission)
          |> List.flatten()

        if length(preload) > 0 do
          [{:preload, preload}]
        else
          []
        end
      end

      defp parse_fields_recv([], map), do: map

      defp parse_fields_recv([f], map) do
        e = Map.get(map, f, %{"_" => []})
        e1 = Map.put(e, "_", [f | e["_"]])
        Map.put(map, f, e1)
      end

      defp parse_fields_recv([f, t], map) do
        e = Map.get(map, f, %{"_" => []})
        e1 = Map.put(e, "_", [t | e["_"]])
        Map.put(map, f, e1)
      end

      defp parse_fields_recv([f, f1, f2], map) do
        e = Map.get(map, f, %{f1 => %{"_" => []}})
        e_f1 = e[f1] || %{"_" => []}
        e1 = Map.put(e_f1, "_", [f2 | e_f1["_"]])
        Map.put(map, f, e1)
      end

      defp parse_fields_recv([f, f1 | t], map) do
        e = Map.get(map, f, %{f1 => []})
        e_f1 = e[f1] || %{}
        list = parse_fields_recv(t, e_f1)
        e1 = Map.put(e, f1, list)
        Map.put(map, f, e1)
      end

      defp recursive_assoc([], _assoc_fields_str, _assoc_modules, _permission), do: []

      defp recursive_assoc([parent], assoc_fields_str, assoc_modules, permission) do
        if parent in assoc_fields_str, do: [String.to_existing_atom(parent)], else: []
      end

      defp recursive_assoc([parent | tail], assoc_fields_str, assoc_modules, permission) do
        if parent in assoc_fields_str do
          [
            deep_assocs(
              String.to_existing_atom(parent),
              tail,
              assoc_modules[String.to_existing_atom(parent)],
              permission
            )
            | recursive_assoc(tail, assoc_fields_str, assoc_modules, permission)
          ]
        else
          [[] | recursive_assoc(tail, assoc_fields_str, assoc_modules, permission)]
        end
      end

      defp deep_assocs(assoc, [], _module, _permission), do: assoc

      defp deep_assocs(assoc, assoc_fields, module, permission) do
        assoc_modules =
          for {f, :relation_type, opt} <- module.__live_fields__, do: {f, opt[:schema]}

        assoc_fields_str = for atom <- module.__assoc_fields__, do: to_string(atom)

        preload =
          recursive_assoc(assoc_fields, assoc_fields_str, assoc_modules, permission)
          |> List.flatten()

        case preload do
          [] -> [assoc]
          preload when is_list(preload) or is_atom(preload) -> [{assoc, [{:preload, preload}]}]
          _ -> []
        end
      end

      defp maybe_add(opt, _key, nil), do: opt
      defp maybe_add(opt, key, value), do: Keyword.put(opt, key, value)

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

  @type p_option ::
          {:limit, integer()}
          | {:offset, integer()}
          | {:permissions, list(atom())}
          | {:assocs, list(atom())}
          | {:filter | list(atom())}
  @type prepare_options :: [p_option()]

  @doc """
  Prepare query from params.

  ### Options
    * `:limit` - Limit of rows
    * `:offset` - Offset of rows.
    * `:permission` - Permission for show fields
    * `:fields` - List fields to return
    * `:assocs` - List assoc to preload
    * `:filter` - Filters

  ## Example
      Post.Api.params_to_query([])
  """
  @callback params_to_query(prepare_options(), struct()) :: list(any())

  ### Functions
  def default_params(_params, _default_schema_, _sc \\ nil)

  def default_params(params, default_schema_, sc) do
    schm_ = sc || default_schema_
    schema_fields = [:id | schm_.bee_raw_fields() |> Keyword.keys()]
    schema_relation_fields = schm_.bee_relation_fields()
    where = params[:where]
    where = if is_map(where), do: Map.to_list(where), else: where
    fields_keys = (where || []) |> Keyword.keys()

    relations_keys =
      (params[:preload] || [])
      |> Enum.map(fn
        {atom, _} -> atom
        atom -> atom
      end)

    # check if all keys are fields
    _ =
      Enum.filter(fields_keys, &(&1 not in schema_fields))
      |> case do
        [] -> nil
        fields -> raise "Fields [#{Enum.join(fields, ", ")}] not present in schema '#{schm_}'"
      end

    # check if all keys are relations
    _ =
      Enum.filter(relations_keys, &(&1 not in schema_relation_fields))
      |> case do
        [] -> nil
        fields -> raise "Fields [#{Enum.join(fields, ", ")}] not present in relations '#{schm_}'"
      end

    sch = from(schm_)

    params
    |> Enum.reduce(sch, fn
      {:where, params}, sch ->
        do_where(params, sch, schm_, nil)
        |> case do
          {query, nil} -> query
          {query, conditions} -> query |> where(^conditions)
        end

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
                [{k, default_params(v, default_schema_, opts[:type])} | acc]
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

  def do_where(params, sch, schm_, parent) do
    relations = for {key, value} <- params, is_list(value), do: {key, value}
    params = for {key, value} <- params, !is_list(value), do: {key, value}
    {query, _} = maybe_join(sch, relations, schm_, parent)
    default_conditions = default_conditions(params, nil, parent)
    {query, default_conditions}
  end

  defp maybe_join(query, params, schm_, parent) do
    relation_fields = schm_.bee_relation_raw_fields()

    {query, _} =
      params
      |> Enum.with_index(1)
      |> Enum.reduce({query, []}, fn {{rel, _wh}, idx}, {query, joins} ->
        {schm_, fk} = relation_fields[rel]

        if is_nil(schm_) do
          query
        else
          params =
            params[rel] |> Keyword.drop([:__COUNT__, :__SUM__, :__MAX__, :__MIN__, :__AVG__])

          #
          vars =
            if is_nil(parent) do
              vars1 = for i <- 0..(idx - 1), do: {:"a#{i}", [], __MODULE__}
              vars1 ++ [{:p, [], nil}]
            else
              [{parent, {:a0, [], nil}}]
            end

          query1 = Macro.escape(query)
          parent_atom = String.to_atom("#{parent || "root"}_#{rel}")

          {query, _} =
            Code.eval_quoted(
              quote do
                import Ecto.Query
                query =
                  join(unquote(query1), :inner, unquote(vars), p in assoc(a0, unquote(rel)),
                    as: unquote(parent_atom)
                  )

                {query1, conditions} =
                  Bee.Api.do_where(unquote(params), query, unquote(schm_), unquote(parent_atom))

                query1
                |> where(^conditions)
              end
            )

          {query, [{rel, schm_, fk}, joins]}
        end
      end)

    {query, nil}
  end

  def default_conditions(params, conditions \\ nil, parent \\ nil) do
    {code, _} =
      Enum.reduce(params, conditions, &default_conditions_map(parent, &1, &2))
      |> Code.eval_quoted()

    code
  end

  defp default_conditions_map(_parent, nil, conditions), do: conditions

  defp default_conditions_map(parent, {key, {:eq, value}}, conditions),
    do: generic_conditions(parent, {key, :==, value}, conditions)

  defp default_conditions_map(parent, {key, {:lt, value}}, conditions),
    do: generic_conditions(parent, {key, :<, value}, conditions)

  defp default_conditions_map(parent, {key, {:elt, value}}, conditions),
    do: generic_conditions(parent, {key, :<=, value}, conditions)

  defp default_conditions_map(parent, {key, {:gt, value}}, conditions),
    do: generic_conditions(parent, {key, :>, value}, conditions)

  defp default_conditions_map(parent, {key, {:egt, value}}, conditions),
    do: generic_conditions(parent, {key, :>=, value}, conditions)

  defp default_conditions_map(parent, {key, {:ilike, value}}, conditions),
    do: generic_conditions(parent, {key, :ilike, "%#{value}%"}, conditions)

  defp default_conditions_map(parent, {key, {:like, value}}, conditions),
    do: generic_conditions(parent, {key, :like, "%#{value}%"}, conditions)

  defp default_conditions_map(parent, {key, {:in, value}}, conditions),
    do: generic_conditions(parent, {key, :in, value}, conditions)

  defp default_conditions_map(parent, {key, {:not, nil}}, conditions),
    do: generic_conditions(parent, {key, :not, nil}, conditions)

  defp default_conditions_map(parent, {key, nil}, conditions),
    do: generic_conditions(parent, {key, :is_nil, nil}, conditions)

  defp default_conditions_map(parent, {key, value}, conditions) do
    generic_conditions(parent, {key, :==, value}, conditions)
  end

  defp generic_conditions(nil, {key, op, value}, nil) do
    args =
      if is_nil(value) do
        [{:field, [], [{:p, [], nil}, key]}]
      else
        [{:field, [], [{:p, [], nil}, key]}, {:^, [], [value]}]
      end

    param = {op, [], args}

    quote do
      dynamic([p], unquote(param))
    end
  end

  defp generic_conditions(nil, {key, op, value}, conditions) do
    args =
      if is_nil(value) do
        [{:field, [], [{:p, [], nil}, key]}]
      else
        [{:field, [], [{:p, [], nil}, key]}, {:^, [], [value]}]
      end

    conditions = {:^, [], [conditions]}
    param = {op, [], args}

    quote do
      dynamic([p], unquote(param) and unquote(conditions))
    end
  end

  defp generic_conditions(parent, {key, op, value}, nil) do
    args =
      if is_nil(value) do
        [{:field, [], [{:p, [], nil}, key]}]
      else
        [{:field, [], [{:p, [], nil}, key]}, {:^, [], [value]}]
      end

    param = {op, [], args}

    quote do
      dynamic([{unquote(parent), p}], unquote(param))
    end
  end

  defp generic_conditions(parent, {key, op, value}, conditions) do
    args =
      if is_nil(value) do
        [{:field, [], [{:p, [], nil}, key]}]
      else
        [{:field, [], [{:p, [], nil}, key]}, {:^, [], [value]}]
      end

    param = {op, [], args}
    conditions = {:^, [], [conditions]}

    quote do
      dynamic([{unquote(parent), p}], unquote(param) and unquote(conditions))
    end
  end
end

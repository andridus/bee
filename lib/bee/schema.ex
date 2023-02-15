defmodule Bee.Schema do
  @moduledoc false

  @live_opts [
    :required,
    :unique,
    :update,
    :size,
    :set_once,
    :to_json,
    :label,
    :opts,
    :applies,
    :validate,
    :precast
  ]
  @relation_opts @live_opts ++
                   [
                     :show,
                     :show_in_form,
                     :parent_field,
                     :form,
                     :options,
                     :class,
                     :relation,
                     :assoc,
                     :default,
                     :schema
                   ]

  defmacro __using__(_) do
    quote do
      use Ecto.Schema
      @behaviour Access
      Module.register_attribute(__MODULE__, :__live_fields__, accumulate: true)
      Module.register_attribute(__MODULE__, :__custom_opts__, [])
      Module.register_attribute(__MODULE__, :__fields_to_json__, accumulate: true)
      Module.register_attribute(__MODULE__, :__required_fields__, accumulate: true)
      Module.register_attribute(__MODULE__, :__update_fields__, accumulate: true)
      Module.register_attribute(__MODULE__, :__unique_fields__, accumulate: true)

      import Ecto.Changeset
      import Bee.Schema
      @before_compile Bee.Schema
      @primary_key {:id, :binary_id, autogenerate: true}
      @foreign_key_type :binary_id

      # Structs by default do not implement this. It's easy to delegate this to the Map implementation however.
    end
  end

  defmacro __before_compile__(%{module: module}) do
    quote do
      module = unquote(module)
      defp maybe_apply_opts(model, field), do: maybe_apply_opts(model, field, :private)
      def __live_fields__(), do: @__live_fields__ |> Enum.reverse()

      def __assoc_fields__(),
        do: @__live_fields__ |> Enum.filter(&elem(&1, 2)[:assoc]) |> Enum.map(&elem(&1, 0))

      def __fields__(), do: @__live_fields__ |> Enum.reverse() |> Enum.map(&elem(&1, 0))
      def __custom_opts__(), do: @__custom_opts__
      def __json__(), do: @__fields_to_json__ |> Enum.reverse() |> Kernel.++([:id])
      def __update_fields__(), do: @__update_fields__ |> Enum.reverse()
      def __required_fields__(), do: @__required_fields__ |> Enum.reverse()
      def __unique_fields__(), do: @__unique_fields__ |> Enum.reverse()

      defp changeset_(model, attrs, :insert) do
        {local_fields, assoc_fields, embed_fields} =
          __MODULE__.__live_fields__()
          |> Enum.reduce({[], [], []}, fn
            {_, :relation_type, _} = item, {fld, assc, embd} -> {fld, [item | assc], embd}
            {_, :embed, _} = item, {fld, assc, embd} -> {fld, assc, [item | embd]}
            item, {fld, assc, embd} -> {[item | fld], assc, embd}
          end)

        assoc_keys = assoc_fields |> Enum.map(&elem(&1, 0))
        embed_keys = embed_fields |> Enum.map(&elem(&1, 0))
        attrs = local_fields |> Enum.reverse() |> precast(attrs)

        flds =
          Enum.reduce(assoc_keys ++ embed_keys, __fields__(), fn k, acc -> List.delete(acc, k) end)

        rfs =
          __required_fields__()
          |> Enum.reduce([], fn
            {field, true}, acc ->
              [field | acc]

            {field, [when: opts]}, acc ->
              Enum.reduce(opts, false, fn
                {k, v}, acc -> Value.get(attrs, k) == v
                _, acc -> acc
              end)
              |> if do
                [field | acc]
              else
                acc
              end
          end)

        model =
          model
          |> cast(attrs, flds)
          |> then(fn model ->
            model =
              Enum.reduce(assoc_fields, model, fn {key, a, opt}, model ->
                schema = opt[:schema]
                cast_assoc(model, key, with: &schema.changeset_insert/2)
              end)

            Enum.reduce(embed_fields, model, fn {key, a, opt}, model ->
              schema = opt[:schema]
              cast_embed(model, key, with: &schema.changeset_insert/2)
            end)
          end)
          |> validate_required(rfs)

        model =
          Enum.reduce(__unique_fields__(), model, fn field, model ->
            unique_constraint(model, field)
          end)

        Enum.reduce(__live_fields__(), model, fn field, model ->
          maybe_apply_opts(model, field)
        end)

        model = %{model | action: :insert}
      end

      defp changeset_(model, attrs, :update) do
        keys = Map.keys(attrs)

        {local_fields, assoc_fields, embed_fields} =
          __MODULE__.__live_fields__()
          # |> Enum.filter(fn
          #   {key, _, _} ->
          #     key in keys
          #   _ -> false
          # end)
          |> Enum.reduce({[], [], []}, fn
            {_, :relation_type, _} = item, {fld, assc, embd} -> {fld, [item | assc], embd}
            {_, :embed, _} = item, {fld, assc, embd} -> {fld, assc, [item | embd]}
            item, {fld, assc, embd} -> {[item | fld], assc, embd}
          end)

        assoc_keys = assoc_fields |> Enum.map(&elem(&1, 0))
        embed_keys = embed_fields |> Enum.map(&elem(&1, 0))
        attrs = local_fields |> Enum.reverse() |> precast(attrs)

        flds =
          Enum.reduce(assoc_keys ++ embed_keys, __fields__(), fn k, acc -> List.delete(acc, k) end)

        rfs =
          __required_fields__()
          |> Enum.reduce([], fn
            {field, true}, acc ->
              [field | acc]

            {field, [when: opts]}, acc ->
              Enum.reduce(opts, false, fn
                {k, v}, acc -> Value.get(attrs, k) == v
                _, acc -> acc
              end)
              |> if do
                [field | acc]
              else
                acc
              end
          end)

        model =
          model
          |> cast(attrs, flds)
          |> then(fn model ->
            Enum.reduce(embed_fields, model, fn {key, a, opt}, model ->
              schema = opt[:schema]
              cast_embed(model, key, with: &schema.changeset_update/2)
            end)
          end)

        model =
          Enum.reduce(__unique_fields__(), model, fn field, model ->
            unique_constraint(model, field)
          end)

        model =
          Enum.reduce(__live_fields__(), model, fn field, model ->
            maybe_apply_opts(model, field)
          end)

        model = %{model | action: :update}
      end

      defdelegate get(coin, key, default), to: Map
      defdelegate fetch(coin, key), to: Map
      defdelegate get_and_update(coin, key, func), to: Map
      defdelegate pop(coin, key), to: Map

      defimpl Jason.Encoder, for: unquote(module) do
        def encode(value, opts) do
          value
          |> Map.drop([:__meta__, :__struct__])
          |> Enum.reject(&match?({_, %Ecto.Association.NotLoaded{}}, &1))
          |> Jason.Encode.keyword(opts)
        end
      end
    end
  end

  def map_excluded_unload_assoc(struct, fields) do
    Map.take(struct, fields)
    |> Map.to_list()
    |> Enum.reduce([], fn {k, v} = kv, acc ->
      if k in struct.__struct__.__assoc_fields__() do
        cond do
          is_list(v) ->
            [kv | acc]

          is_map(v) && v != %Ecto.Association.NotLoaded{} && Map.get(v, :__struct__) == nil ->
            [kv | acc]

          is_map(v) && Ecto.assoc_loaded?(struct[k]) ->
            [kv | acc]

          :else ->
            acc
        end
      else
        [kv | acc]
      end
    end)
    |> Map.new()
  end

  defmacro many_to_many_(name, queryable, opts \\ []) do
    queryable = expand_alias(queryable, __CALLER__)
    opts = expand_alias_in_key(opts, :join_through, __CALLER__)
    opts = opts ++ [relation: :many_to_many, default: [], assoc: true]
    only_opts = get_opts(opts, @relation_opts)

    quote do
      Module.put_attribute(
        __MODULE__,
        :__live_fields__,
        {unquote(name), :relation_type, unquote(opts) ++ [schema: unquote(queryable)]}
      )

      field = {:many_to_many, unquote(name), unquote(queryable), unquote(opts)}

      Ecto.Schema.__many_to_many__(
        __MODULE__,
        unquote(name),
        unquote(queryable),
        unquote(only_opts)
      )
    end
  end

  defmacro belongs_to_(name, queryable, opts \\ []) do
    queryable = expand_alias(queryable, __CALLER__)
    opts = opts ++ [relation: :belongs_to, default: nil, assoc: true]
    only_opts = get_opts(opts, @relation_opts)

    quote do
      Module.put_attribute(
        __MODULE__,
        :__live_fields__,
        {unquote(name), :relation_type, unquote(opts) ++ [schema: unquote(queryable)]}
      )

      field = {:belongs_to, unquote(name), unquote(queryable), unquote(opts)}

      Ecto.Schema.__belongs_to__(
        __MODULE__,
        unquote(name),
        unquote(queryable),
        unquote(only_opts)
      )
    end
  end

  defmacro has_one_(name, queryable, opts \\ []) do
    queryable = expand_alias(queryable, __CALLER__)
    opts = opts ++ [relation: :has_one, default: nil, assoc: true]
    only_opts = get_opts(opts, @relation_opts)

    quote do
      Module.put_attribute(
        __MODULE__,
        :__live_fields__,
        {unquote(name), :relation_type, unquote(opts) ++ [schema: unquote(queryable)]}
      )

      field = {:has_one, unquote(name), unquote(queryable), unquote(opts)}
      Ecto.Schema.__has_one__(__MODULE__, unquote(name), unquote(queryable), unquote(only_opts))
    end
  end

  defmacro has_many_(name, queryable, opts \\ []) do
    queryable = expand_alias(queryable, __CALLER__)
    opts = opts ++ [relation: :has_many, default: [], schema: queryable, assoc: true]
    only_opts = get_opts(opts, @relation_opts)

    quote do
      Module.put_attribute(
        __MODULE__,
        :__live_fields__,
        {unquote(name), :relation_type, unquote(opts)}
      )

      field = {:has_many, unquote(name), unquote(queryable), unquote(opts)}
      Ecto.Schema.__has_many__(__MODULE__, unquote(name), unquote(queryable), unquote(only_opts))
    end
  end

  defmacro embeds_many_(name, queryable, opts \\ []) do
    queryable = expand_alias(queryable, __CALLER__)
    opts = opts ++ [default: [], schema: queryable]
    only_opts = get_opts(opts, @relation_opts)

    quote do
      Module.put_attribute(__MODULE__, :__live_fields__, {unquote(name), :embed, unquote(opts)})
      field = {:embeds_many, unquote(name), unquote(queryable), unquote(opts)}

      Ecto.Schema.__embeds_many__(
        __MODULE__,
        unquote(name),
        unquote(queryable),
        unquote(only_opts)
      )
    end
  end

  defmacro embeds_one_(name, queryable, opts \\ []) do
    queryable = expand_alias(queryable, __CALLER__)
    opts = opts ++ [default: nil, schema: queryable]
    only_opts = get_opts(opts, @relation_opts)

    quote do
      Module.put_attribute(__MODULE__, :__live_fields__, {unquote(name), :embed, unquote(opts)})

      field = {:embeds_one, unquote(name), unquote(queryable), unquote(opts)}

      Ecto.Schema.__embeds_one__(
        __MODULE__,
        unquote(name),
        unquote(queryable),
        unquote(only_opts)
      )
    end
  end

  defmacro timestamps_(opts \\ []) do
    quote do
      timestamps = Keyword.merge(@timestamps_opts, unquote(opts))
      inserted_at = Keyword.get(timestamps, :inserted_at, :inserted_at)
      updated_at = Keyword.get(timestamps, :updated_at, :updated_at)

      Module.put_attribute(
        __MODULE__,
        :__live_fields__,
        {inserted_at, :timestamp, [timestamp: true]}
      )

      Module.put_attribute(
        __MODULE__,
        :__live_fields__,
        {updated_at, :timestamp, [timestamp: true]}
      )

      timestamps()
    end
  end

  defmacro custom_opts(opts \\ []) do
    quote do
      Module.put_attribute(__MODULE__, :__custom_opts__, unquote(opts))
    end
  end

  defmacro field_(name, type \\ :string, opts \\ []),
    do: parse_field_(__CALLER__, name, type, opts)

  def parse_field_(%{module: module}, name, type, opts) do
    only_opts = get_opts(opts)

    quote do
      module = unquote(module)
      name = unquote(name)
      type = unquote(type)
      opts = unquote(opts)

      if opts[:required],
        do: Module.put_attribute(module, :__required_fields__, {name, opts[:required]})

      if opts[:unique], do: Module.put_attribute(module, :__unique_fields__, name)
      if !opts[:json], do: Module.put_attribute(module, :__fields_to_json__, name)

      if is_nil(opts[:update]) || opts[:update] == true,
        do: Module.put_attribute(module, :__update_fields__, name)

      Module.put_attribute(module, :__live_fields__, {name, type, opts})

      if !!opts[:relation] == false,
        do: Ecto.Schema.__field__(module, name, type, unquote(only_opts))
    end
  end

  defp get_opts(opts, default \\ @live_opts) do
    removable_fields =
      Keyword.keys(opts)
      |> Enum.filter(
        &(&1
          |> to_string()
          |> String.starts_with?("_"))
      )
      |> Kernel.++(default)
      |> Enum.uniq()

    Keyword.drop(opts, removable_fields)
  end

  def precast(fields, attrs) do
    Enum.reduce(fields, attrs, fn {_field, _type, opts}, attrs ->
      (opts[:precast] || [])
      |> Enum.reduce(attrs, fn
        {module, function, args}, attrs ->
          apply(module, function, [attrs] ++ args)

        _, attrs ->
          attrs
      end)
    end)
  end

  def maybe_apply_opts(model, {field, type, opts}, :private) do
    applies = opts[:applies] || []

    model =
      Enum.reduce(applies, model, fn
        {module, function, args}, model ->
          apply(module, function, [model] ++ args)

        _, model ->
          model
      end)

    validates = opts[:validate] || []

    Enum.reduce(validates, model, fn
      {:format, value}, model ->
        model |> Ecto.Changeset.validate_format(field, value)

      {:function, {module, function}}, model ->
        apply(module, function, [model, field, type])

      _, model ->
        model
    end)
  end

  defp expand_alias({:__aliases__, _, _} = ast, env),
    do: Macro.expand(ast, %{env | function: {:__schema__, 2}})

  defp expand_alias(ast, _env),
    do: ast

  defp expand_alias_in_key(opts, key, env) do
    if is_list(opts) and Keyword.has_key?(opts, key) do
      Keyword.update!(opts, key, &expand_alias(&1, env))
    else
      opts
    end
  end
end

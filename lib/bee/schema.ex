defmodule Bee.Schema do
  @moduledoc """
    Generate a Bee.Schema from valid ecto schema.
  """
  defmacro __using__(opts) do
    foreign_key_type = opts[:foreign_key_type] || :integer

    timestamp_opts =
      opts[:timestamp_opts] ||
        [
          type: :naive_datetime,
          inserted_at: :inserted_at,
          updated_at: :updated_at
        ]

    primary_key = opts[:primary_key] || {:id, :integer, autogenerate: true}

    Module.register_attribute(__CALLER__.module, :bee_timestamp_opts, [])
    Module.register_attribute(__CALLER__.module, :bee_foreign_key_type, [])
    Module.register_attribute(__CALLER__.module, :bee_primary_key, [])
    Module.put_attribute(__CALLER__.module, :bee_foreign_key_type, foreign_key_type)
    Module.put_attribute(__CALLER__.module, :bee_primary_key, primary_key)
    Module.put_attribute(__CALLER__.module, :bee_timestamp_opts, timestamp_opts)

    quote do
      Module.register_attribute(__MODULE__, :bee_permission_def, accumulate: true)
      @behaviour Access
      require Bee.Schema
      import Bee.Schema
      @before_compile Bee.Schema
    end
  end

  defmacro __before_compile__(%{module: module}) do
    permissions = Module.get_attribute(module, :bee_permission_def)

    funcs =
      for {atom, list, extends} <- permissions do
        if !is_nil(extends) and is_atom(extends) do
          quote do
            def bee_permission(unquote(atom)),
              do:
                [bee_permission(unquote(extends)) ++ unquote(list)]
                |> List.flatten()
                |> Enum.uniq()
          end
        else
          quote do
            def bee_permission(unquote(atom)), do: unquote(list) |> Enum.uniq()
          end
        end
      end

    quote do
      def bee_permission(nil), do: bee_json()
      unquote(funcs)
      def bee_permission(atom), do: raise("Permission '#{atom}' dont exists")

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

  @relation_tags [:belongs_to, :has_one, :has_many, :many_to_many]
  @embed_tags [:embeds_many, :embeds_one]
  @fields_tags @relation_tags ++ @embed_tags ++ [:field, :timestamps]
  defp map_opts(type_of, field, line, opts, type, acc, bee_foreign_key_type, module_fk) do
    bee_opts = opts[:bee] || []

    other_attributes =
      opts |> Enum.filter(&(elem(&1, 0) |> to_string() |> String.starts_with?("__")))

    other_attributes_k = Keyword.keys(other_attributes)
    bee_opts = bee_opts ++ other_attributes
    opts = Keyword.drop(opts, [:bee | other_attributes_k])
    mapped_opts = opts ++ bee_opts ++ [type: type, type_of: type_of]

    result =
      case type_of do
        :belongs_to ->
          fk = opts[:foreign_key] || :"#{field}_id"
          opts_foreign_key = Keyword.drop(mapped_opts, [:type]) ++ [type: bee_foreign_key_type]

          opts_relation =
            Keyword.drop(mapped_opts, [:required]) ++ [relation: true, foreign_key: fk]

          [{:{}, [], [fk, opts_foreign_key]}, {:{}, [], [field, opts_relation]} | acc]

        typeof when typeof in @relation_tags ->
          fk = opts[:foreign_key] || module_fk
          opts = mapped_opts ++ [relation: true, foreign_key: fk]
          [{:{}, [], [field, opts]} | acc]

        typeof when typeof in @embed_tags ->
          [{:{}, [], [field, mapped_opts ++ [embed: true]]} | acc]

        _ ->
          [{:{}, [], [field, mapped_opts]} | acc]
      end

    {{type_of, line, [field, type, opts]}, result}
  end

  defp map_timestamps(line, opts, bee_timestamp_opts, acc) do
    bee_opts = opts[:bee] || []

    other_attributes =
      opts |> Enum.filter(&(elem(&1, 0) |> to_string() |> String.starts_with?("__")))

    other_attributes_k = Keyword.keys(other_attributes)
    bee_opts = bee_opts ++ other_attributes
    opts = Keyword.drop(opts, [:bee | other_attributes_k])

    type = bee_timestamp_opts[:type]
    inserted_at = bee_timestamp_opts[:inserted_at] || :inserted_at
    updated_at = bee_timestamp_opts[:updated_at] || :updated_at
    mapped_opts = opts ++ bee_opts ++ [type: type, type_of: :timestamps, timestamps: true]
    result = [{:{}, [], [inserted_at, mapped_opts]}, {:{}, [], [updated_at, mapped_opts]} | acc]
    {{:timestamps, line, opts}, result}
  end

  @typedoc "Abstract Syntax Tree (AST)"
  @type t :: expr | literal

  @typedoc "Represents expressions in the AST"
  @type expr :: {expr | atom, keyword, atom | [t]}

  @typedoc "Represents literals in the AST"
  @type literal :: atom | number | binary | fun | {t, t} | [t]

  @spec permission(atom(), list(atom())) :: Macro.t()
  defmacro permission(atom, list) do
    atom = Macro.escape(atom)
    list = Macro.escape(list)

    quote do
      Module.put_attribute(__MODULE__, :bee_permission_def, {unquote(atom), unquote(list), nil})
    end
  end

  @spec permission(atom(), list(atom()), keyword()) :: Macro.t()
  defmacro permission(atom, list, opts) do
    atom = Macro.escape(atom)
    list = Macro.escape(list)
    extends = Macro.escape(opts[:extends])

    quote do
      Module.put_attribute(
        __MODULE__,
        :bee_permission_def,
        {unquote(atom), unquote(list), unquote(extends)}
      )
    end
  end

  def convert_module_to_atom(nil), do: nil

  def convert_module_to_atom(module),
    do:
      module
      |> Module.split()
      |> Enum.reverse()
      |> List.first()
      |> String.downcase()
      |> String.to_atom()

  def convert_module_to_foreign_key(nil), do: nil

  def convert_module_to_foreign_key(module),
    do:
      module
      |> Module.split()
      |> Enum.reverse()
      |> List.first()
      |> String.downcase()
      |> Kernel.<>("_id")
      |> String.to_atom()

  @spec generate_bee(t()) :: Macro.t()
  defmacro generate_bee(ast) do
    ast = ast[:do] || []
    module = __CALLER__.module
    bee_foreign_key_type = Module.get_attribute(module, :bee_foreign_key_type)
    bee_timestamp_opts = Module.get_attribute(module, :bee_timestamp_opts)
    module_fk = convert_module_to_foreign_key(module)

    {primary_key_id, primary_key_type, primary_key_opts} =
      Module.get_attribute(module, :bee_primary_key)

    primary_key =
      Macro.escape({primary_key_id, primary_key_opts |> Keyword.put(:type, primary_key_type)})

    {ast, raw_fields} =
      Macro.postwalk(ast, [], fn
        {typeof, line, [field, type, opts]}, acc when typeof in @fields_tags ->
          map_opts(typeof, field, line, opts, type, acc, bee_foreign_key_type, module_fk)

        {typeof, line, [field, type]}, acc when typeof in @fields_tags ->
          map_opts(typeof, field, line, [], type, acc, bee_foreign_key_type, module_fk)

        {:timestamps, line, opts}, acc ->
          map_timestamps(line, opts, bee_timestamp_opts, acc)

        any, acc ->
          {any, acc}
      end)

    quote do
      def bee_primary_key, do: unquote(primary_key)
      def bee_raw_fields, do: unquote(raw_fields)

      def bee_timestamps,
        do: for({field, opt} <- unquote(raw_fields), opt[:timestamps], do: field)

      def bee_required_fields,
        do: for({field, opt} <- unquote(raw_fields), opt[:required], do: field)

      def bee_not_update_fields,
        do: for({field, opt} <- unquote(raw_fields), opt[:dont_update], do: field)

      def bee_relation_fields,
        do: for({field, opt} <- unquote(raw_fields), opt[:relation], do: field)

      def bee_relation_raw_fields,
        do:
          for(
            {field, opt} <- unquote(raw_fields),
            opt[:relation],
            do: {field, {opt[:type], opt[:foreign_key]}}
          )

      def bee_embed_fields, do: for({field, opt} <- unquote(raw_fields), opt[:embed], do: field)

      def bee_embed_raw_fields,
        do: for({field, opt} <- unquote(raw_fields), opt[:embed], do: {field, opt[:type], opt})

      def bee_fields,
        do:
          for({field, _opt} <- unquote(raw_fields), do: field) --
            bee_relation_fields() -- bee_timestamps()

      def bee_json, do: [:id | bee_fields()] ++ bee_timestamps()

      def changeset_(model, attrs, :insert) do
        raw_fields = bee_raw_fields()
        fields = bee_fields()
        assoc_fields = bee_relation_fields()
        embed_fields = bee_embed_fields()
        raw_embed_fields = bee_embed_raw_fields()
        required_fields = bee_required_fields()

        flds = fields -- embed_fields

        model
        |> Ecto.Changeset.cast(attrs, flds)
        |> Ecto.Changeset.validate_required(required_fields)
        |> (fn model ->
              Enum.reduce(raw_embed_fields, model, fn {key, entity, _}, model ->
                Ecto.Changeset.cast_embed(model, key, with: &entity.changeset_insert/2)
              end)
            end).()
        |> Map.put(:action, :insert)
      end

      def changeset_(model, attrs, :update) do
        raw_fields = bee_raw_fields()

        assoc_fields = bee_relation_fields()
        embed_fields = bee_embed_fields()
        not_update_fields = bee_not_update_fields()
        required_fields = bee_required_fields() -- not_update_fields
        raw_embed_fields = bee_embed_raw_fields()
        fields = bee_fields() -- not_update_fields

        flds = fields -- embed_fields

        model =
          model
          |> Ecto.Changeset.cast(attrs, flds)
          |> Ecto.Changeset.validate_required(required_fields)
          |> (fn model ->
                Enum.reduce(raw_embed_fields, model, fn {key, entity, _}, model ->
                  Ecto.Changeset.cast_embed(model, key, with: &entity.changeset_update/2)
                end)
              end).()
          |> Map.put(:action, :update)
      end

      def changeset_insert(model, attrs), do: changeset_(model, attrs, :insert)

      def changeset_update(model, attrs), do: changeset_(model, attrs, :update)

      unquote(ast)

      defoverridable changeset_insert: 2, changeset_update: 2
    end
  end
end

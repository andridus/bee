defmodule Bee.Schema do
  @moduledoc """
    Generate a Bee.Schema from valid ecto schema.
  """
  defmacro __using__(_) do
    quote do
      require Bee.Schema
      import Bee.Schema
    end
  end

  @relation_tags [:belongs_to, :has_one, :has_many, :many_to_many]
  @embed_tags [:embeds_many, :embeds_one]
  @fields_tags @relation_tags ++ @embed_tags ++ [:field, :timestamps]
  defp map_opts(type_of, field, opts, type, acc) do
    bee_opts = opts[:bee] || []
    opts = Keyword.drop(opts, [:bee])

    mapped_opts = opts ++ bee_opts ++ [type: type, type_of: type_of]

    case type_of do
      :belongs_to ->
        fk = opts[:foreign_key] || :"#{field}_id"
        [{:{}, [], [fk, mapped_opts]}, {:{}, [], [field, mapped_opts ++ [relation: true]]} | acc]

      typeof when typeof in @relation_tags ->
        [{:{}, [], [field, mapped_opts ++ [relation: true]]} | acc]

      typeof when typeof in @embed_tags ->
        [{:{}, [], [field, mapped_opts ++ [embed: true]]} | acc]

      _ ->
        [{:{}, [], [field, mapped_opts]} | acc]
    end
  end

  @typedoc "Abstract Syntax Tree (AST)"
  @type t :: expr | literal

  @typedoc "Represents expressions in the AST"
  @type expr :: {expr | atom, keyword, atom | [t]}

  @typedoc "Represents literals in the AST"
  @type literal :: atom | number | binary | fun | {t, t} | [t]

  @spec generate_bee(t()) :: Macro.t()
  defmacro generate_bee(ast) do
    ast = ast[:do] || []

    {_ast, raw_fields} =
      Macro.postwalk(ast, [], fn
        {typeof, line, [field, type, opts]}, acc when typeof in @fields_tags ->
          {{typeof, line, [field, type, opts]}, map_opts(typeof, field, [], type, acc)}

        {typeof, line, [field, type]}, acc when typeof in @fields_tags ->
          {{typeof, line, [field, type]}, map_opts(typeof, field, [], type, acc)}

        any, acc ->
          {any, acc}
      end)

    quote do
      def bee_raw_fields, do: unquote(raw_fields)

      def bee_required_fields,
        do: for({field, opt} <- unquote(raw_fields), opt[:required], do: field)

      def bee_not_update_fields,
        do: for({field, opt} <- unquote(raw_fields), opt[:dont_update], do: field)

      def bee_relation_fields,
        do: for({field, opt} <- unquote(raw_fields), opt[:relation], do: field)

      def bee_embed_fields, do: for({field, opt} <- unquote(raw_fields), opt[:embed], do: field)

      def bee_fields,
        do: for({field, _opt} <- unquote(raw_fields), do: field) -- bee_relation_fields()

      defp changeset_(model, attrs, :insert) do
        fields = bee_raw_fields()
        assoc_fields = bee_relation_fields()
        embed_fields = bee_embed_fields()
        required_fields = bee_required_fields()

        # flds = for {f, opt} <- bee_raw_fields, f in (fields ++ embed_fields), do: {f, opt}
        flds = fields ++ embed_fields

        model
        |> Ecto.Changeset.cast(attrs, flds)
        |> Ecto.Changeset.validate_required(required_fields)
        |> Map.put(:action, :insert)
      end

      defp changeset_(model, attrs, :insert) do
        fields = bee_raw_fields()
        assoc_fields = bee_relation_fields()
        embed_fields = bee_embed_fields()
        required_fields = bee_required_fields()
        not_update_fields = bee_not_update_fields()

        # flds = for {f, opt} <- bee_raw_fields, f in (fields ++ embed_fields), do: {f, opt}
        flds = fields ++ (embed_fields -- not_update_fields)
        required_fields = required_fields -- not_update_fields

        model
        |> Ecto.Changeset.cast(attrs, flds)
        |> Ecto.Changeset.validate_required(required_fields)
        |> Map.put(:action, :update)
      end

      def changeset_insert(model, attrs), do: changeset_(model, attrs, :insert)
      def changeset_update(model, attrs), do: changeset_(model, attrs, :update)
      unquote(ast)

      defoverridable changeset_insert: 2, changeset_update: 2
    end
  end
end

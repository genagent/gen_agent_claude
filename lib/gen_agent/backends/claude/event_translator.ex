defmodule GenAgent.Backends.Claude.EventTranslator do
  @moduledoc """
  Translates `ClaudeWrapper.StreamEvent` values into `GenAgent.Event` values.

  The Claude CLI's `stream-json` output surfaces a variety of event types
  ("system", "assistant", "content_block_delta", "tool_use", "tool_result",
  "result", "error", etc). The GenAgent state machine only cares about a
  small normalized set (`:text`, `:tool_use`, `:tool_result`, `:usage`,
  `:result`, `:error`).

  This module is a pure function of a single `StreamEvent` to a list of
  zero or more `GenAgent.Event` values. Callers stream events through
  `translate/1` (typically via `Stream.flat_map/2`).

  ## Translation rules

    * `"system"` -- filtered out (no GenAgent events).
    * `"assistant"` -- any text content blocks become a single `:text` event
      carrying the concatenated text. An assistant message with no text
      content is filtered out.
    * `"content_block_delta"` -- emits a `:text` event with the delta's text.
    * `"tool_use"` -- emits a `:tool_use` event carrying the raw data map.
    * `"tool_result"` -- emits a `:tool_result` event carrying the raw data map.
    * `"result"` -- emits a `:usage` event (if token counts are present
      under `data["usage"]`) followed by a terminal `:result` event with
      `:text`, `:session_id`, and any additional Claude-specific
      metadata (`cost_usd`, `duration_ms`, `num_turns`, `is_error`)
      passed through in `:data`. Claude reports cost as `total_cost_usd`;
      we normalize it to `:cost_usd`.
    * `"error"` -- emits a terminal `:error` event with `:reason` extracted
      from `data["error"]` or `data["message"]`.
    * Unknown types -- filtered out.
  """

  alias ClaudeWrapper.StreamEvent
  alias GenAgent.Event

  @doc """
  Translate a single `StreamEvent` into zero or more `GenAgent.Event` values.
  """
  @spec translate(StreamEvent.t()) :: [Event.t()]
  def translate(%StreamEvent{type: "system"}), do: []

  def translate(%StreamEvent{type: "assistant", data: data}) do
    case extract_assistant_text(data) do
      "" -> []
      text -> [Event.new(:text, %{text: text})]
    end
  end

  def translate(%StreamEvent{type: "content_block_delta", data: %{"delta" => %{"text" => text}}})
      when is_binary(text) do
    [Event.new(:text, %{text: text})]
  end

  def translate(%StreamEvent{type: "content_block_delta"}), do: []

  def translate(%StreamEvent{type: "tool_use", data: data}) do
    [Event.new(:tool_use, data)]
  end

  def translate(%StreamEvent{type: "tool_result", data: data}) do
    [Event.new(:tool_result, data)]
  end

  def translate(%StreamEvent{type: "result", data: data}) do
    usage_event =
      case extract_usage(data) do
        nil -> []
        usage -> [Event.new(:usage, usage)]
      end

    event_data =
      %{
        text: data["result"] || "",
        session_id: data["session_id"],
        cost_usd: data["total_cost_usd"] || data["cost_usd"],
        duration_ms: data["duration_ms"],
        num_turns: data["num_turns"],
        is_error: data["is_error"] || false
      }
      |> drop_nil_values()

    usage_event ++ [Event.new(:result, event_data)]
  end

  def translate(%StreamEvent{type: "error", data: data}) do
    reason = data["error"] || data["message"] || :unknown
    [Event.new(:error, %{reason: reason, data: data})]
  end

  def translate(%StreamEvent{}), do: []

  @doc """
  Translate an enumerable of `StreamEvent` values into an enumerable of
  `GenAgent.Event` values, flattening empty translations.
  """
  @spec translate_stream(Enumerable.t()) :: Enumerable.t()
  def translate_stream(stream) do
    Stream.flat_map(stream, &translate/1)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp extract_assistant_text(%{"message" => %{} = message}) do
    extract_assistant_text(message)
  end

  defp extract_assistant_text(%{"content" => content}) when is_list(content) do
    content
    |> Enum.map_join("", fn
      %{"text" => text} when is_binary(text) -> text
      %{"type" => "text", "text" => text} when is_binary(text) -> text
      _ -> ""
    end)
  end

  defp extract_assistant_text(%{"content" => content}) when is_binary(content), do: content

  defp extract_assistant_text(_), do: ""

  defp drop_nil_values(map) do
    map
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new()
  end

  defp extract_usage(%{"usage" => %{} = usage}) do
    input = usage["input_tokens"]
    output = usage["output_tokens"]

    case {input, output} do
      {nil, nil} ->
        nil

      _ ->
        %{input_tokens: input, output_tokens: output}
        |> drop_nil_values()
    end
  end

  defp extract_usage(_), do: nil
end

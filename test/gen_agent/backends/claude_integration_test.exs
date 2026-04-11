defmodule GenAgent.Backends.ClaudeIntegrationTest do
  @moduledoc """
  End-to-end tests that drive a real `GenAgent` process with the
  `GenAgent.Backends.Claude` backend, but with the `ClaudeWrapper`
  subprocess stubbed out via an injected `stream_fn`.

  These tests exercise the full state-machine path:
  `GenAgent.start_agent/2` -> `GenAgent.ask/2` -> `Claude.prompt/2` ->
  fake stream -> `EventTranslator` -> back into the state machine and
  delivered as a `GenAgent.Response`.

  They are the second consumer of `GenAgent.Backend`, after the in-tree
  mock. If either end of that contract drifts, these break.
  """

  use ExUnit.Case, async: true

  alias ClaudeWrapper.StreamEvent

  defmodule ClaudeAgent do
    use GenAgent

    defmodule State do
      defstruct responses: []
    end

    @impl true
    def init_agent(opts) do
      backend_opts = Keyword.take(opts, [:stream_fn, :working_dir, :cwd, :model, :system_prompt])
      {:ok, backend_opts, %State{}}
    end

    @impl true
    def handle_response(_ref, response, %State{} = state) do
      {:noreply, %{state | responses: state.responses ++ [response]}}
    end
  end

  defp stream_event(type, data), do: %StreamEvent{type: type, data: data, raw: ""}

  defp unique_name(prefix) do
    "#{prefix}-#{System.unique_integer([:positive])}"
  end

  defp start_claude_agent(stream_fn, extra_opts \\ []) do
    name = unique_name("claude")

    {:ok, _pid} =
      GenAgent.start_agent(
        ClaudeAgent,
        [
          name: name,
          backend: GenAgent.Backends.Claude,
          stream_fn: stream_fn
        ] ++ extra_opts
      )

    on_exit(fn ->
      case GenAgent.whereis(name) do
        nil -> :ok
        _ -> GenAgent.stop(name)
      end
    end)

    name
  end

  describe "round trip through GenAgent.ask/2" do
    test "assembles a Response from the translated stream" do
      stream_fn = fn _prompt, _opts ->
        [
          stream_event("system", %{"info" => "init"}),
          stream_event("assistant", %{
            "content" => [%{"type" => "text", "text" => "the answer is 42"}]
          }),
          stream_event("result", %{
            "result" => "the answer is 42",
            "session_id" => "sess-abc",
            "cost_usd" => 0.001,
            "duration_ms" => 123
          })
        ]
      end

      name = start_claude_agent(stream_fn)

      assert {:ok, response} = GenAgent.ask(name, "what is the answer?")
      assert response.text == "the answer is 42"
      assert response.session_id == "sess-abc"
      assert Enum.map(response.events, & &1.kind) == [:text, :result]
    end

    test "threads session_id across multiple turns via --resume" do
      test_pid = self()

      stream_fn = fn prompt, opts ->
        send(test_pid, {:claude_call, prompt, Keyword.get(opts, :resume)})

        [
          stream_event("result", %{
            "result" => "ack: #{prompt}",
            "session_id" => "sess-persist"
          })
        ]
      end

      name = start_claude_agent(stream_fn)

      {:ok, _} = GenAgent.ask(name, "turn 1")
      assert_receive {:claude_call, "turn 1", nil}

      {:ok, _} = GenAgent.ask(name, "turn 2")
      assert_receive {:claude_call, "turn 2", "sess-persist"}

      {:ok, _} = GenAgent.ask(name, "turn 3")
      assert_receive {:claude_call, "turn 3", "sess-persist"}
    end

    test "forwards init_agent opts (model, system_prompt) to the stream_fn" do
      test_pid = self()

      stream_fn = fn _prompt, opts ->
        send(test_pid, {:claude_opts, opts})
        [stream_event("result", %{"result" => "ok"})]
      end

      name =
        start_claude_agent(stream_fn,
          model: "sonnet",
          system_prompt: "You are a test agent.",
          cwd: "/tmp/test"
        )

      {:ok, _} = GenAgent.ask(name, "hello")

      assert_receive {:claude_opts, opts}
      assert opts[:model] == "sonnet"
      assert opts[:system_prompt] == "You are a test agent."
      assert opts[:working_dir] == "/tmp/test"
    end

    test "delivers a :no_terminal_event error when the stream ends without a result" do
      stream_fn = fn _prompt, _opts ->
        [stream_event("assistant", %{"content" => [%{"type" => "text", "text" => "partial"}]})]
      end

      name = start_claude_agent(stream_fn)

      assert {:error, :no_terminal_event} = GenAgent.ask(name, "go")
    end

    test "delivers an error terminal event through the state machine" do
      stream_fn = fn _prompt, _opts ->
        [
          stream_event("error", %{"error" => "rate limited"})
        ]
      end

      name = start_claude_agent(stream_fn)

      assert {:error, "rate limited"} = GenAgent.ask(name, "boom")
    end
  end
end

defmodule Pigeon.FCM.Config do
  @moduledoc "FCM Configuration for Pigeon"

  defstruct uri: 'fcm.googleapis.com',
            port: 443,
            name: nil,
            project_id: nil,
            auth_mfa: nil

  @type t :: %__MODULE__{
          name: term,
          port: pos_integer,
          uri: charlist,
          project_id: binary,
          auth_mfa: {atom, atom, list(term)}
        }

  @doc ~S"""
  Returns a new `FCM.Config` with given `opts`.

  ## Examples

      iex> Pigeon.FCM.Config.new(
      ...>   name: :test,
      ...>   uri: 'test.server.example.com',
      ...>   port: 5228
      ...>   project_id: "my-project-id",
      ...>   auth_mfa: {Goth, :fetch!, [MyApp.Goth]}
      ...> )
      %Pigeon.FCM.Config{name: :test,
      port: 5228, uri: 'test.server.example.com'...}
  """
  def new(opts) when is_list(opts) do
    %__MODULE__{
      name: opts[:name],
      uri: Keyword.get(opts, :uri, 'fcm.googleapis.com'),
      port: Keyword.get(opts, :port, 443),
      project_id: Keyword.fetch!(opts, :project_id),
      auth_mfa: Keyword.fetch!(opts, :auth_mfa)
    }
  end

  def new(name) when is_atom(name) do
    Application.get_env(:pigeon, :fcm)[name]
    |> Enum.to_list()
    |> Keyword.put(:name, name)
    |> new()
  end
end

defimpl Pigeon.Configurable, for: Pigeon.FCM.Config do
  @moduledoc false

  require Logger

  import Pigeon.Tasks, only: [process_on_response: 2]

  alias Pigeon.Encodable
  alias Pigeon.FCM.Config

  @type sock :: {:sslsocket, any, pid | {any, any}}

  # Configurable Callbacks

  @spec worker_name(any) :: atom | nil
  def worker_name(%Config{name: name}), do: name

  @spec max_demand(any) :: non_neg_integer
  def max_demand(_config), do: 100

  @spec connect(any) :: {:ok, sock} | {:error, String.t()}
  def connect(%Config{uri: uri} = config) do
    case connect_socket_options(config) do
      {:ok, options} ->
        Pigeon.Http2.Client.default().connect(uri, :https, options)
    end
  end

  def connect_socket_options(config) do
    opts =
      [
        {:active, :once},
        {:packet, :raw},
        {:reuseaddr, true},
        {:alpn_advertised_protocols, [<<"h2">>]},
        :binary
      ]
      |> add_port(config)

    {:ok, opts}
  end

  def add_port(opts, %Config{port: 443}), do: opts
  def add_port(opts, %Config{port: port}), do: [{:port, port} | opts]

  def push_headers(%Config{project_id: project_id, auth_mfa: {m, f, a}}, _notification, _opts) do
    token = Kernel.apply(m, f, a).token
    [
      {":method", "POST"},
      {":path", "/v1/projects/#{project_id}/messages:send"},
      {"authorization", "Bearer #{token}"},
      {"content-type", "application/json"},
      {"accept", "application/json"}
    ]
  end

  def push_payload(_config, notification, _opts) do
    Encodable.binary_payload(notification)
  end

  def handle_end_stream(_config, %{error: nil} = stream, notif, on_response) do
    do_handle_end_stream(stream.status, stream.body, notif, on_response)
  end

  def handle_end_stream(_config, %{error: _error}, _notif, nil), do: :ok

  def handle_end_stream(_config, _stream, {_regids, notif}, on_response) do
    notif = %{notif | status: :unavailable}
    process_on_response(on_response, notif)
  end

  defp do_handle_end_stream(200, body, notif, on_response) do
    result = Pigeon.json_library().decode!(body)
    notif = %{notif | status: :success}
    parse_result(notif.registration_id, result, on_response, notif)
  end

  defp do_handle_end_stream(400, _body, notif, on_response) do
    log_error("400", "Malformed JSON")
    notif = %{notif | status: :malformed_json}
    process_on_response(on_response, notif)
  end

  defp do_handle_end_stream(401, _body, notif, on_response) do
    log_error("401", "Unauthorized")
    notif = %{notif | status: :unauthorized}
    process_on_response(on_response, notif)
  end

  defp do_handle_end_stream(500, _body, notif, on_response) do
    log_error("500", "Internal server error")
    notif = %{notif | status: :internal_server_error}
    process_on_response(on_response, notif)
  end

  defp do_handle_end_stream(code, body, notif, on_response) do
    reason = parse_error(body)
    log_error(code, reason)
    notif = %{notif | response: reason}
    process_on_response(on_response, notif)
  end

  def schedule_ping(_config), do: :ok

  def close(_config) do
  end

  def validate!(%{project_id: project_id, auth_mfa: {_m, _f, _a}}) when is_binary(project_id) do
    :ok
  end

  def validate!(config) do
    raise Pigeon.ConfigError,
      reason: "attempted to start without valid project_id and/or auth_mfa",
      config: config
  end

  # no on_response callback, ignore
  def parse_result(_id, _response, nil, _notif), do: :ok

  def parse_result(_id, %{"name" => _name}, on_response, notif) do
    process_on_response(on_response, %{ notif | response: :success})
  end

  def parse_result(_id, %{"error" => error}, on_response, notif) do
    process_on_response(on_response, %{ notif | response: parse_error(error)})
  end

  def parse_error(data) do
    case Pigeon.json_library().decode(data) do
      {:ok, response} ->
        response["reason"] |> Macro.underscore() |> String.to_existing_atom()

      error ->
        "JSON parse failed: #{inspect(error)}, body: #{inspect(data)}"
        |> Logger.error()
    end
  end

  defp log_error(code, reason) do
    if Pigeon.debug_log?(), do: Logger.error("#{reason}: #{code}")
  end
end

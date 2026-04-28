from __future__ import annotations

import inspect
import json
import logging
import os
from queue import Empty
from typing import Any, Optional, Union

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from graph_agent.api.manager import GraphRunManager


_log_level_name = os.environ.get("GRAPH_AGENT_LOG_LEVEL", "").strip().upper()
if _log_level_name:
    _level = getattr(logging, _log_level_name, None)
    if isinstance(_level, int):
        _graph_agent_logger = logging.getLogger("graph_agent")
        _graph_agent_logger.setLevel(_level)
        if not _graph_agent_logger.handlers:
            _handler = logging.StreamHandler()
            _handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
            _graph_agent_logger.addHandler(_handler)
            _graph_agent_logger.propagate = False


class RunDocumentPayload(BaseModel):
    document_id: str
    name: str
    mime_type: str = "application/octet-stream"
    size_bytes: int = 0
    storage_path: str = ""
    text_content: str = ""
    text_excerpt: str = ""
    status: str = "ready"
    error: Optional[str] = None


class ProjectFilePayload(BaseModel):
    file_id: str
    graph_id: str
    name: str
    mime_type: str = "application/octet-stream"
    size_bytes: int = 0
    storage_path: str = ""
    status: str = "ready"
    created_at: str = ""
    error: Optional[str] = None


class RunRequest(BaseModel):
    input: Any
    agent_ids: Optional[list[str]] = None
    documents: Optional[list[RunDocumentPayload]] = None
    graph_env_vars: Optional[dict[str, str]] = None


class ProviderPreflightRequest(BaseModel):
    provider_name: str
    provider_config: Optional[dict[str, Any]] = None
    live: bool = False


class SpreadsheetPreviewRequest(BaseModel):
    file_path: str
    file_format: str = "auto"
    sheet_name: Optional[str] = None
    header_row_index: int = 1
    start_row_index: Optional[Union[int, str]] = 2
    empty_row_policy: str = "skip"


class SupabaseSchemaPreviewRequest(BaseModel):
    supabase_url_env_var: str = "GRAPH_AGENT_SUPABASE_URL"
    supabase_key_env_var: str = "GRAPH_AGENT_SUPABASE_SECRET_KEY"
    schema_name: str = Field(default="public", alias="schema")
    graph_env_vars: Optional[dict[str, str]] = None


class SupabaseRuntimeStatusRequest(BaseModel):
    supabase_url_env_var: str = "GRAPH_AGENT_SUPABASE_URL"
    supabase_key_env_var: str = "GRAPH_AGENT_SUPABASE_SECRET_KEY"
    graph_env_vars: Optional[dict[str, str]] = None


class OutboundEmailLogTableValidationRequest(BaseModel):
    supabase_url_env_var: str = "GRAPH_AGENT_SUPABASE_URL"
    supabase_key_env_var: str = "GRAPH_AGENT_SUPABASE_SECRET_KEY"
    schema_name: str = Field(default="public", alias="schema")
    table_name: str = ""
    graph_env_vars: Optional[dict[str, str]] = None


class SupabaseAuthVerifyRequest(BaseModel):
    supabase_url: str
    supabase_key: str
    schema_name: str = Field(default="public", alias="schema")
    project_ref: str = ""
    access_token: str = ""


class MicrosoftDeviceCodeStartRequest(BaseModel):
    client_id: str
    tenant_id: str
    scopes: Optional[list[str]] = None


class GraphPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    graph_id: str
    name: str
    description: str = ""
    version: str = "1.0"
    graph_type: Optional[str] = None
    email_routing_mode: Optional[str] = None
    default_input: Optional[str] = None
    env_vars: Optional[dict[str, str]] = None
    start_node_id: Optional[str] = None
    nodes: Optional[list[dict[str, Any]]] = None
    edges: Optional[list[dict[str, Any]]] = None
    agents: Optional[list[dict[str, Any]]] = None


class ToolToggleRequest(BaseModel):
    enabled: bool


class McpServerPayload(BaseModel):
    server_id: str
    display_name: str
    description: str = ""
    transport: str = "stdio"
    command: list[str] = []
    cwd: Optional[str] = None
    env: Optional[dict[str, str]] = None
    headers: Optional[dict[str, str]] = None
    base_url: Optional[str] = None
    timeout_seconds: int = 15
    auto_boot: bool = False
    persistent: bool = True


app = FastAPI(title="Graph Agent API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^http://(localhost|127\.0\.0\.1|\[::1\]):\d+$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

manager = GraphRunManager()


@app.on_event("startup")
def startup_event() -> None:
    manager.start_background_services()


@app.on_event("shutdown")
def shutdown_event() -> None:
    manager.stop_background_services()


@app.get("/api/graphs")
def list_graphs() -> dict[str, Any]:
    return {"graphs": manager.list_graphs()}


@app.get("/api/graphs/{graph_id}")
def get_graph(graph_id: str) -> dict[str, Any]:
    try:
        return manager.get_graph(graph_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown graph '{graph_id}'.") from exc


@app.post("/api/graphs")
def create_graph(graph: GraphPayload) -> dict[str, Any]:
    try:
        return manager.create_graph(graph.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/graphs/{graph_id}")
def update_graph(graph_id: str, graph: GraphPayload) -> dict[str, Any]:
    try:
        return manager.update_graph(graph_id, graph.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown graph '{graph_id}'.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/graphs/{graph_id}")
def delete_graph(graph_id: str) -> dict[str, str]:
    try:
        manager.delete_graph(graph_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown graph '{graph_id}'.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": graph_id}


@app.get("/api/graphs/{graph_id}/files")
def list_project_files(graph_id: str) -> dict[str, Any]:
    try:
        return {"files": manager.list_project_files(graph_id)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/graphs/{graph_id}/files/upload")
async def upload_project_files(graph_id: str, request: Request) -> dict[str, Any]:
    try:
        form = await request.form()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=503,
            detail=(
                "Document uploads require multipart support. Install the `python-multipart` package "
                "in the same environment as the API server, then restart. "
                f"({type(exc).__name__}: {exc})"
            ),
        ) from exc
    files = form.getlist("files")
    if not files:
        raise HTTPException(status_code=400, detail="Select at least one file to upload.")
    payloads: list[dict[str, Any]] = []
    for upload in files:
        filename = getattr(upload, "filename", None)
        read = getattr(upload, "read", None)
        close = getattr(upload, "close", None)
        if not callable(read):
            continue
        try:
            payloads.append(
                {
                    "name": filename or "file",
                    "content_type": getattr(upload, "content_type", None),
                    "data": await read(),
                }
            )
        finally:
            if callable(close):
                try:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
                except Exception:  # noqa: BLE001
                    pass
    if not payloads:
        raise HTTPException(status_code=400, detail="Select at least one file to upload.")
    try:
        project_files = manager.upload_project_files(graph_id, payloads)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"files": project_files}


@app.delete("/api/graphs/{graph_id}/files/{file_id}")
def delete_project_file(graph_id: str, file_id: str) -> dict[str, str]:
    try:
        manager.delete_project_file(graph_id, file_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown project file '{file_id}'.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": file_id}


@app.get("/api/graphs/{graph_id}/files/{file_id}/content")
def read_project_file_content(graph_id: str, file_id: str) -> dict[str, Any]:
    try:
        return manager.read_project_file_content(graph_id, file_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown project file '{file_id}'.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/editor/catalog")
def get_editor_catalog() -> dict[str, Any]:
    return manager.get_catalog()


@app.post("/api/editor/providers/preflight")
def preflight_provider(request: ProviderPreflightRequest) -> dict[str, Any]:
    try:
        return manager.preflight_provider(
            request.provider_name,
            request.provider_config,
            live=request.live,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown provider '{request.provider_name}'.") from exc


@app.post("/api/editor/providers/diagnostics")
def provider_diagnostics(request: ProviderPreflightRequest) -> dict[str, Any]:
    try:
        return manager.provider_diagnostics(
            request.provider_name,
            request.provider_config,
            live=request.live,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown provider '{request.provider_name}'.") from exc


@app.post("/api/editor/data/spreadsheet/preview")
def preview_spreadsheet_rows(request: SpreadsheetPreviewRequest) -> dict[str, Any]:
    try:
        return manager.preview_spreadsheet_rows(request.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/data/supabase/schema")
def preview_supabase_schema(request: SupabaseSchemaPreviewRequest) -> dict[str, Any]:
    try:
        return manager.preview_supabase_schema(request.model_dump(by_alias=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/data/supabase/status")
def inspect_supabase_runtime(request: SupabaseRuntimeStatusRequest) -> dict[str, Any]:
    try:
        return manager.inspect_supabase_runtime(request.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/data/supabase/outbound-email-log/validate")
def validate_outbound_email_log_table(request: OutboundEmailLogTableValidationRequest) -> dict[str, Any]:
    try:
        return manager.validate_outbound_email_log_table(request.model_dump(by_alias=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/data/supabase/auth/verify")
def verify_supabase_auth(request: SupabaseAuthVerifyRequest) -> dict[str, Any]:
    try:
        return manager.verify_supabase_auth(request.model_dump(by_alias=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/editor/integrations/microsoft/status")
def get_microsoft_auth_status() -> dict[str, Any]:
    try:
        return manager.get_microsoft_auth_status()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/integrations/microsoft/device/start")
def start_microsoft_device_code(request: MicrosoftDeviceCodeStartRequest) -> dict[str, Any]:
    try:
        return manager.start_microsoft_device_code(request.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/editor/integrations/microsoft")
def disconnect_microsoft_auth() -> dict[str, Any]:
    try:
        return manager.disconnect_microsoft_auth()
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/documents/upload")
async def upload_run_documents(request: Request) -> dict[str, Any]:
    try:
        form = await request.form()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=503,
            detail=(
                "Document uploads require multipart support. Install the `python-multipart` package "
                "in the same environment as the API server, then restart. "
                f"({type(exc).__name__}: {exc})"
            ),
        ) from exc
    files = form.getlist("files")
    if not files:
        raise HTTPException(status_code=400, detail="Select at least one document to upload.")
    payloads: list[dict[str, Any]] = []
    for upload in files:
        filename = getattr(upload, "filename", None)
        read = getattr(upload, "read", None)
        close = getattr(upload, "close", None)
        if not callable(read):
            continue
        try:
            payloads.append(
                {
                    "name": filename or "document",
                    "content_type": getattr(upload, "content_type", None),
                    "data": await read(),
                }
            )
        finally:
            if callable(close):
                try:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
                except Exception:  # noqa: BLE001
                    pass
    if not payloads:
        raise HTTPException(status_code=400, detail="Select at least one document to upload.")
    try:
        documents = manager.upload_run_documents(payloads)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"documents": documents}


@app.post("/api/editor/mcp/servers/{server_id}/boot")
def boot_mcp_server(server_id: str) -> dict[str, Any]:
    try:
        return manager.boot_mcp_server(server_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown MCP server '{server_id}'.") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/mcp/servers/{server_id}/stop")
def stop_mcp_server(server_id: str) -> dict[str, Any]:
    try:
        return manager.stop_mcp_server(server_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown MCP server '{server_id}'.") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/mcp/servers/{server_id}/refresh")
def refresh_mcp_server(server_id: str) -> dict[str, Any]:
    try:
        return manager.refresh_mcp_server(server_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown MCP server '{server_id}'.") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/mcp/servers")
def create_mcp_server(server: McpServerPayload) -> dict[str, Any]:
    try:
        return manager.create_mcp_server(server.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.put("/api/editor/mcp/servers/{server_id}")
def update_mcp_server(server_id: str, server: McpServerPayload) -> dict[str, Any]:
    try:
        return manager.update_mcp_server(server_id, server.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown MCP server '{server_id}'.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/editor/mcp/servers/{server_id}")
def delete_mcp_server(server_id: str) -> dict[str, str]:
    try:
        manager.delete_mcp_server(server_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown MCP server '{server_id}'.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": server_id}


@app.post("/api/editor/mcp/servers/test")
def test_mcp_server(server: McpServerPayload) -> dict[str, Any]:
    try:
        return manager.test_mcp_server(server.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/editor/mcp/tools/{tool_name}/toggle")
def toggle_mcp_tool(tool_name: str, request: ToolToggleRequest) -> dict[str, Any]:
    try:
        return manager.set_mcp_tool_enabled(tool_name, request.enabled)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown tool '{tool_name}'.") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/graphs/{graph_id}/runs")
def list_graph_runs(graph_id: str, limit: int = 50) -> dict[str, Any]:
    return {"runs": manager.list_runs(graph_id, limit=limit)}


@app.post("/api/graphs/{graph_id}/runs")
def start_run(graph_id: str, request: RunRequest) -> dict[str, str]:
    try:
        run_id = manager.start_run(
            graph_id,
            request.input,
            agent_ids=request.agent_ids,
            documents=[document.model_dump() for document in request.documents or []],
            graph_env_vars=request.graph_env_vars,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown graph '{graph_id}'.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"run_id": run_id}


@app.post("/api/runtime/reset")
def reset_runtime() -> dict[str, Any]:
    return manager.reset_runtime()


@app.post("/api/runtime/stop")
def stop_runtime() -> dict[str, Any]:
    return manager.stop_runtime()


@app.get("/api/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    try:
        return manager.get_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown run '{run_id}'.") from exc


@app.get("/api/runs/{run_id}/status")
def get_run_status(run_id: str) -> dict[str, Any]:
    """Lightweight status check; never pulls state_snapshot or events.

    Used by the frontend SSE-fallback poller to avoid re-recovering full
    run state on every poll tick.
    """
    try:
        return manager.get_run_status(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown run '{run_id}'.") from exc


@app.get("/api/runs/{run_id}/files")
def list_run_files(run_id: str) -> dict[str, Any]:
    try:
        return manager.list_run_files(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown run '{run_id}'.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/runs/{run_id}/files/content")
def read_run_file(run_id: str, path: str) -> dict[str, Any]:
    try:
        return manager.read_run_file(run_id, path)
    except KeyError as exc:
        detail = str(exc).strip("'") or f"File '{path}' was not found."
        raise HTTPException(status_code=404, detail=detail) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/runs/{run_id}/events")
def stream_run_events(run_id: str) -> StreamingResponse:
    try:
        backlog, queue = manager.subscribe(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown run '{run_id}'.") from exc

    def event_stream():
        try:
            for event in backlog:
                yield f"data: {json.dumps(event)}\n\n"

            while True:
                try:
                    item = queue.get(timeout=15)
                except Empty:
                    yield ": keep-alive\n\n"
                    continue

                if item is None:
                    break
                yield f"data: {item}\n\n"
        finally:
            manager.unsubscribe(run_id, queue)

    return StreamingResponse(event_stream(), media_type="text/event-stream")

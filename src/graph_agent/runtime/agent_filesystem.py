from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import mimetypes
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any


DEFAULT_AGENT_FILESYSTEM_ROOT = Path(__file__).resolve().parents[3] / ".graph-agent" / "runs"
DEFAULT_FILE_READ_CHAR_LIMIT = 100_000
SAFE_WORKSPACE_SEGMENT_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


class AgentFilesystemError(ValueError):
    """Raised when a sandboxed agent filesystem request is invalid."""


@dataclass(frozen=True)
class AgentWorkspaceRef:
    root: Path
    run_id: str
    agent_id: str

    @property
    def workspace_dir(self) -> Path:
        return self.root / self.run_id / "agents" / self.agent_id / "workspace"


def resolve_agent_filesystem_root() -> Path:
    configured = os.environ.get("GRAPH_AGENT_WORKSPACE_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_AGENT_FILESYSTEM_ROOT


def resolve_agent_workspace(run_id: str, agent_id: str | None, *, create: bool = False) -> AgentWorkspaceRef:
    normalized_run_id = _sanitize_workspace_segment(run_id, fallback="run")
    normalized_agent_id = _sanitize_workspace_segment(agent_id or "default", fallback="default")
    workspace = AgentWorkspaceRef(
        root=resolve_agent_filesystem_root(),
        run_id=normalized_run_id,
        agent_id=normalized_agent_id,
    )
    if create:
        workspace.workspace_dir.mkdir(parents=True, exist_ok=True)
    return workspace


def normalize_workspace_relative_path(relative_path: str) -> PurePosixPath:
    raw_value = str(relative_path or "").strip().replace("\\", "/")
    if not raw_value:
        raise AgentFilesystemError("A relative file path is required.")
    normalized = PurePosixPath(raw_value)
    if normalized.is_absolute():
        raise AgentFilesystemError("Absolute file paths are not allowed in the agent workspace.")
    parts = [part for part in normalized.parts if part not in {"", "."}]
    if not parts:
        raise AgentFilesystemError("A relative file path is required.")
    if any(part == ".." for part in parts):
        raise AgentFilesystemError("Parent directory traversal is not allowed in the agent workspace.")
    return PurePosixPath(*parts)


def resolve_agent_workspace_path(
    run_id: str,
    agent_id: str | None,
    relative_path: str,
    *,
    create_parent: bool = False,
) -> tuple[AgentWorkspaceRef, PurePosixPath, Path]:
    workspace = resolve_agent_workspace(run_id, agent_id, create=create_parent)
    normalized_relative_path = normalize_workspace_relative_path(relative_path)
    if create_parent:
        workspace.workspace_dir.mkdir(parents=True, exist_ok=True)
    target_path = (workspace.workspace_dir / Path(*normalized_relative_path.parts)).resolve()
    workspace_root = workspace.workspace_dir.resolve()
    if target_path != workspace_root and workspace_root not in target_path.parents:
        raise AgentFilesystemError("Resolved workspace path escaped the agent sandbox.")
    if create_parent:
        target_path.parent.mkdir(parents=True, exist_ok=True)
    return workspace, normalized_relative_path, target_path


def write_agent_workspace_text_file(
    run_id: str,
    agent_id: str | None,
    relative_path: str,
    content: str,
) -> dict[str, Any]:
    workspace, normalized_relative_path, target_path = resolve_agent_workspace_path(
        run_id,
        agent_id,
        relative_path,
        create_parent=True,
    )
    target_path.write_text(content, encoding="utf-8")
    return describe_agent_workspace_file(workspace, target_path, relative_path=normalized_relative_path.as_posix())


def list_agent_workspace_files(run_id: str, agent_id: str | None) -> dict[str, Any]:
    workspace = resolve_agent_workspace(run_id, agent_id, create=True)
    files = [
        describe_agent_workspace_file(workspace, candidate)
        for candidate in sorted(workspace.workspace_dir.rglob("*"))
        if candidate.is_file()
    ]
    return {
        "run_id": run_id,
        "agent_id": agent_id or "default",
        "workspace_root": str(workspace.workspace_dir),
        "files": files,
    }


def read_agent_workspace_file(
    run_id: str,
    agent_id: str | None,
    relative_path: str,
    *,
    char_limit: int = DEFAULT_FILE_READ_CHAR_LIMIT,
) -> dict[str, Any]:
    workspace, normalized_relative_path, target_path = resolve_agent_workspace_path(run_id, agent_id, relative_path)
    if not target_path.exists() or not target_path.is_file():
        raise FileNotFoundError(normalized_relative_path.as_posix())
    raw_bytes = target_path.read_bytes()
    text_content = raw_bytes.decode("utf-8", errors="replace")
    truncated = False
    if len(text_content) > char_limit:
        text_content = text_content[:char_limit]
        truncated = True
    return {
        **describe_agent_workspace_file(workspace, target_path, relative_path=normalized_relative_path.as_posix()),
        "content": text_content,
        "truncated": truncated,
        "encoding": "utf-8",
    }


def describe_agent_workspace_file(
    workspace: AgentWorkspaceRef,
    path: Path,
    *,
    relative_path: str | None = None,
) -> dict[str, Any]:
    resolved_path = path.resolve()
    workspace_root = workspace.workspace_dir.resolve()
    if resolved_path != workspace_root and workspace_root not in resolved_path.parents:
        raise AgentFilesystemError("Resolved workspace path escaped the agent sandbox.")
    stat = resolved_path.stat()
    relative = relative_path or resolved_path.relative_to(workspace_root).as_posix()
    mime_type = mimetypes.guess_type(resolved_path.name)[0] or "text/plain"
    return {
        "path": relative,
        "name": resolved_path.name,
        "size_bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "mime_type": mime_type,
    }


def _sanitize_workspace_segment(value: str, *, fallback: str) -> str:
    normalized = SAFE_WORKSPACE_SEGMENT_PATTERN.sub("-", str(value or "").strip()).strip("-.")
    return normalized or fallback

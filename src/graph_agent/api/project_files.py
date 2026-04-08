from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
from typing import Any, Iterable, Mapping
from uuid import uuid4


DEFAULT_PROJECT_FILE_STORAGE_DIR = Path(__file__).resolve().parents[3] / ".graph-agent" / "project-files"
SUPPORTED_PROJECT_FILE_EXTENSIONS = {".csv", ".json", ".md", ".markdown", ".pdf", ".txt", ".xlsx"}
MANIFEST_FILENAME = "manifest.json"


class ProjectFileError(ValueError):
    pass


@dataclass
class ProjectFileRecord:
    file_id: str
    graph_id: str
    name: str
    mime_type: str
    size_bytes: int
    storage_path: str
    status: str
    created_at: str
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_id": self.file_id,
            "graph_id": self.graph_id,
            "name": self.name,
            "mime_type": self.mime_type,
            "size_bytes": self.size_bytes,
            "storage_path": self.storage_path,
            "status": self.status,
            "created_at": self.created_at,
            "error": self.error,
        }


def resolve_project_file_storage_dir() -> Path:
    configured = os.environ.get("GRAPH_AGENT_PROJECT_FILE_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_PROJECT_FILE_STORAGE_DIR


class ProjectFileStore:
    def __init__(self, root: Path | None = None) -> None:
        self._root = root or resolve_project_file_storage_dir()
        self._root.mkdir(parents=True, exist_ok=True)

    def list_files(self, graph_id: str) -> list[dict[str, Any]]:
        normalized_graph_id = _normalize_graph_id(graph_id)
        records = self._load_manifest(normalized_graph_id)
        cleaned_records = self._prune_missing_files(normalized_graph_id, records)
        return [record.to_dict() for record in cleaned_records]

    def upload_files(self, graph_id: str, files: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        normalized_graph_id = _normalize_graph_id(graph_id)
        graph_dir = self._graph_dir(normalized_graph_id)
        graph_dir.mkdir(parents=True, exist_ok=True)
        records = self._load_manifest(normalized_graph_id)
        uploaded: list[ProjectFileRecord] = []
        for file_payload in files:
            record = self._store_file(normalized_graph_id, graph_dir, file_payload)
            records.append(record)
            uploaded.append(record)
        self._save_manifest(normalized_graph_id, records)
        return [record.to_dict() for record in uploaded]

    def delete_file(self, graph_id: str, file_id: str) -> None:
        normalized_graph_id = _normalize_graph_id(graph_id)
        normalized_file_id = str(file_id).strip()
        if not normalized_file_id:
            raise ProjectFileError("Project file id is required.")
        records = self._load_manifest(normalized_graph_id)
        next_records: list[ProjectFileRecord] = []
        deleted_record: ProjectFileRecord | None = None
        for record in records:
            if record.file_id == normalized_file_id and deleted_record is None:
                deleted_record = record
                continue
            next_records.append(record)
        if deleted_record is None:
            raise KeyError(normalized_file_id)
        storage_path = Path(deleted_record.storage_path)
        if storage_path.exists():
            storage_path.unlink()
        self._save_manifest(normalized_graph_id, next_records)
        graph_dir = self._graph_dir(normalized_graph_id)
        if graph_dir.exists() and not any(graph_dir.iterdir()):
            graph_dir.rmdir()

    def rename_graph(self, previous_graph_id: str, next_graph_id: str) -> None:
        old_graph_id = _normalize_graph_id(previous_graph_id)
        new_graph_id = _normalize_graph_id(next_graph_id)
        if old_graph_id == new_graph_id:
            return
        old_dir = self._graph_dir(old_graph_id)
        if not old_dir.exists():
            return
        new_dir = self._graph_dir(new_graph_id)
        if new_dir.exists():
            merged_records = self._load_manifest(new_graph_id)
            for record in self._load_manifest(old_graph_id):
                original_path = Path(record.storage_path)
                if original_path.exists():
                    target_path = new_dir / original_path.name
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(original_path), str(target_path))
                    merged_records.append(
                        ProjectFileRecord(
                            **{
                                **record.to_dict(),
                                "graph_id": new_graph_id,
                                "storage_path": str(target_path),
                            }
                        )
                    )
            self._save_manifest(new_graph_id, merged_records)
            shutil.rmtree(old_dir, ignore_errors=True)
            return
        old_dir.rename(new_dir)
        records = self._load_manifest(new_graph_id)
        updated_records = [
            ProjectFileRecord(
                **{
                    **record.to_dict(),
                    "graph_id": new_graph_id,
                    "storage_path": str(new_dir / Path(record.storage_path).name),
                }
            )
            for record in records
        ]
        self._save_manifest(new_graph_id, updated_records)

    def delete_graph(self, graph_id: str) -> None:
        normalized_graph_id = _normalize_graph_id(graph_id)
        graph_dir = self._graph_dir(normalized_graph_id)
        if graph_dir.exists():
            shutil.rmtree(graph_dir, ignore_errors=True)

    def _store_file(self, graph_id: str, graph_dir: Path, file_payload: Mapping[str, Any]) -> ProjectFileRecord:
        name = str(file_payload.get("name") or "").strip()
        if not name:
            raise ProjectFileError("Project file name is required.")
        raw_bytes = file_payload.get("data")
        if not isinstance(raw_bytes, (bytes, bytearray)):
            raise ProjectFileError(f"Project file '{name}' is missing file bytes.")
        extension = Path(name).suffix.strip().lower()
        if extension not in SUPPORTED_PROJECT_FILE_EXTENSIONS:
            raise ProjectFileError(
                "Unsupported file type. Upload .txt, .md, .markdown, .json, .csv, .xlsx, or .pdf files."
            )
        file_id = uuid4().hex
        safe_name = _safe_filename(name)
        storage_path = graph_dir / f"{file_id}-{safe_name}"
        storage_path.write_bytes(bytes(raw_bytes))
        return ProjectFileRecord(
            file_id=file_id,
            graph_id=graph_id,
            name=name,
            mime_type=str(file_payload.get("content_type") or "").strip() or "application/octet-stream",
            size_bytes=len(raw_bytes),
            storage_path=str(storage_path),
            status="ready",
            created_at=datetime.now(timezone.utc).isoformat(),
            error=None,
        )

    def _graph_dir(self, graph_id: str) -> Path:
        return self._root / _graph_directory_name(graph_id)

    def _manifest_path(self, graph_id: str) -> Path:
        return self._graph_dir(graph_id) / MANIFEST_FILENAME

    def _load_manifest(self, graph_id: str) -> list[ProjectFileRecord]:
        manifest_path = self._manifest_path(graph_id)
        if not manifest_path.exists():
            return []
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(payload, list):
            return []
        records: list[ProjectFileRecord] = []
        for candidate in payload:
            if not isinstance(candidate, dict):
                continue
            file_id = str(candidate.get("file_id") or "").strip()
            name = str(candidate.get("name") or "").strip()
            if not file_id or not name:
                continue
            records.append(
                ProjectFileRecord(
                    file_id=file_id,
                    graph_id=str(candidate.get("graph_id") or graph_id),
                    name=name,
                    mime_type=str(candidate.get("mime_type") or "application/octet-stream"),
                    size_bytes=_coerce_non_negative_int(candidate.get("size_bytes")),
                    storage_path=str(candidate.get("storage_path") or ""),
                    status=str(candidate.get("status") or "ready"),
                    created_at=str(candidate.get("created_at") or ""),
                    error=_coerce_optional_string(candidate.get("error")),
                )
            )
        return records

    def _save_manifest(self, graph_id: str, records: list[ProjectFileRecord]) -> None:
        manifest_path = self._manifest_path(graph_id)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps([record.to_dict() for record in records], indent=2),
            encoding="utf-8",
        )

    def _prune_missing_files(self, graph_id: str, records: list[ProjectFileRecord]) -> list[ProjectFileRecord]:
        kept_records = [record for record in records if Path(record.storage_path).exists()]
        if len(kept_records) != len(records):
            self._save_manifest(graph_id, kept_records)
        return kept_records


def _normalize_graph_id(graph_id: str) -> str:
    normalized = str(graph_id).strip()
    if not normalized:
        raise ProjectFileError("Graph id is required.")
    return normalized


def _graph_directory_name(graph_id: str) -> str:
    digest = hashlib.sha1(graph_id.encode("utf-8")).hexdigest()[:12]
    safe_graph_id = _safe_filename(graph_id)
    return f"{safe_graph_id}-{digest}"


def _safe_filename(name: str) -> str:
    base_name = Path(name).name.strip() or "file"
    safe = "".join(character if character.isalnum() or character in {".", "_", "-"} else "-" for character in base_name)
    safe = safe.strip("-.")
    return safe or "file"


def _coerce_non_negative_int(value: Any) -> int:
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return 0


def _coerce_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None

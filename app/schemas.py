"""Response models (also drive the OpenAPI / Swagger documentation)."""
from __future__ import annotations

from pydantic import BaseModel, Field


class Rect(BaseModel):
    x0: float
    y0: float
    x1: float
    y1: float


class HitSpan(BaseModel):
    block_id: str = Field(..., description="OCR block containing this part of the hit")
    page: int = Field(..., description="1-based scan page number")
    rect: Rect = Field(..., description="block rectangle on the scan image")
    char_start: int = Field(
        ..., description="start offset of the highlight inside the original block text"
    )
    char_end: int = Field(
        ..., description="exclusive end offset inside the original block text"
    )
    block_order: int = Field(..., description="reading-order index of the block")


class Hit(BaseModel):
    compact_start: int = Field(..., description="hit start in the whitespace-stripped text")
    compact_end: int = Field(..., description="exclusive hit end in the stripped text")
    matched_text: str = Field(..., description="original characters covered by the hit")
    crosses_blocks: bool
    crosses_pages: bool
    spans: list[HitSpan] = Field(
        ..., description="block ids, pages, rectangles and original char ranges for highlighting"
    )


class SearchResponse(BaseModel):
    query: str
    compact_query: str
    version_id: int
    limit: int
    result_count: int
    has_more: bool = Field(..., description="more than `limit` matches exist")
    hits: list[Hit]


class DocumentInfo(BaseModel):
    doc_id: str
    active_version_id: int | None
    active_state: str | None
    block_count: int | None
    updated_at: float


class VersionInfo(BaseModel):
    version_id: int
    doc_id: str
    state: str
    created_at: float
    finalized_at: float | None
    block_count: int
    error: str | None


class ReplacementAccepted(BaseModel):
    doc_id: str
    version_id: int
    state: str
    detail: str


class WithdrawalItem(BaseModel):
    version_id: int = Field(..., description="pre-withdrawal version whose tables are reclaimed")
    state: str = Field(..., description="pending (awaiting drop) or done (tables removed)")
    updated_at: float


class WithdrawalReceipt(BaseModel):
    withdrawal_id: str = Field(..., description="the submitted withdrawal number (撤档号)")
    doc_id: str
    epoch: int = Field(..., description="withdrawn document generation; re-archives start a new epoch")
    state: str = Field(
        ...,
        description="accepted -> reclaiming -> completed (or failed, auto-retried)",
    )
    replayed: bool = Field(
        ..., description="true when the withdrawal number was already known (idempotent replay)"
    )
    detail: str | None
    error: str | None = Field(
        None, description="last reclaim error, if any; cleared on the successful retry"
    )
    created_at: float
    updated_at: float
    completed_at: float | None
    items: list[WithdrawalItem] = Field(
        ..., description="durable reclaim scope: every pre-withdrawal version"
    )


class RecoveryReport(BaseModel):
    recovered: list[str]


class Health(BaseModel):
    status: str
    db_path: str

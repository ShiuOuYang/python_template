from __future__ import annotations

"""Excel 相關 API — 讀取、寫入、驗證、合併、匯出。"""

import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse

from app.schemas.base import ApiResponse
from app.utils.excel_handler import ExcelHandler
from app.utils.excel_validation import ColumnRule, DataType

router = APIRouter(prefix="/excel", tags=["Excel"])

# 示範用固定路徑；實務上應改為上傳檔或從 DB / 設定取得
SAMPLE_FILE = Path(__file__).resolve().parents[4] / "output_examples" / "02_multi_sheet.xlsx"


def _run_sync(func, *args, **kwargs):  # type: ignore[no-untyped-def]
    """在 executor 中執行同步函式，避免阻塞事件迴圈。"""
    import functools

    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, functools.partial(func, *args, **kwargs))


# ======================================================================
# 1. 取得 Sheet 清單
# ======================================================================

@router.get("/sheets", response_model=ApiResponse)
async def get_sheets() -> ApiResponse:
    """取得 Excel 檔案中所有 Sheet 的名稱與基本資訊。"""
    if not SAMPLE_FILE.exists():
        raise HTTPException(status_code=404, detail="範例檔案不存在")

    handler = ExcelHandler(SAMPLE_FILE)
    info = await _run_sync(handler.get_sheet_info)
    return ApiResponse(data=info, message=f"共 {len(info)} 個工作表")


# ======================================================================
# 2. 讀取指定 Sheet 資料
# ======================================================================

@router.get("/sheets/{sheet_name}", response_model=ApiResponse)
async def read_sheet(
    sheet_name: str,
    limit: int = Query(default=100, ge=1, le=10000, description="回傳最多幾筆"),
    offset: int = Query(default=0, ge=0, description="跳過幾筆"),
) -> ApiResponse:
    """讀取指定 Sheet，支援分頁。"""
    if not SAMPLE_FILE.exists():
        raise HTTPException(status_code=404, detail="範例檔案不存在")

    handler = ExcelHandler(SAMPLE_FILE)
    try:
        df = await _run_sync(handler.read_sheet, sheet_name=sheet_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"讀取失敗：{e}")

    total = len(df)
    page_df = df.iloc[offset: offset + limit]
    records = page_df.where(page_df.notna(), None).to_dict(orient="records")

    return ApiResponse(
        data={
            "total": total,
            "offset": offset,
            "limit": limit,
            "columns": list(df.columns),
            "rows": records,
        },
        message=f"Sheet '{sheet_name}' 共 {total} 行，回傳第 {offset + 1}~{min(offset + limit, total)} 筆",
    )


# ======================================================================
# 3. 偵測欄位資訊
# ======================================================================

@router.get("/sheets/{sheet_name}/columns", response_model=ApiResponse)
async def detect_columns(sheet_name: str) -> ApiResponse:
    """偵測指定 Sheet 中每個欄位的名稱、推斷型別、非空率。"""
    if not SAMPLE_FILE.exists():
        raise HTTPException(status_code=404, detail="範例檔案不存在")

    handler = ExcelHandler(SAMPLE_FILE)
    try:
        columns = await _run_sync(handler.detect_columns, sheet_name=sheet_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"偵測失敗：{e}")

    return ApiResponse(data=columns, message=f"共偵測到 {len(columns)} 個欄位")


# ======================================================================
# 4. 驗證資料
# ======================================================================

@router.post("/sheets/{sheet_name}/validate", response_model=ApiResponse)
async def validate_sheet(
    sheet_name: str,
    rules: List[Dict[str, Any]],
) -> ApiResponse:
    """
    驗證指定 Sheet 的資料。

    Request Body 範例：
    ```json
    [
        {"name": "月份", "data_type": "string", "required": true},
        {"name": "營收", "data_type": "integer", "required": true, "min_value": 0}
    ]
    ```
    """
    if not SAMPLE_FILE.exists():
        raise HTTPException(status_code=404, detail="範例檔案不存在")

    # 將 dict 轉為 ColumnRule
    column_rules: List[ColumnRule] = []
    for r in rules:
        column_rules.append(ColumnRule(
            name=r["name"],
            data_type=DataType(r.get("data_type", "string")),
            required=r.get("required", False),
            min_value=r.get("min_value"),
            max_value=r.get("max_value"),
            min_length=r.get("min_length"),
            max_length=r.get("max_length"),
            allowed_values=r.get("allowed_values"),
            regex_pattern=r.get("regex_pattern"),
        ))

    handler = ExcelHandler(SAMPLE_FILE)
    df = await _run_sync(handler.read_sheet, sheet_name=sheet_name)
    result = await _run_sync(handler.validate, df, column_rules)

    errors_data = [
        {
            "row": e.row,
            "column": e.column,
            "value": str(e.value) if e.value is not None else None,
            "error_type": e.error_type,
            "message": e.message,
        }
        for e in result.errors
    ]

    return ApiResponse(
        data={
            "is_valid": result.is_valid,
            "total_rows": result.total_rows,
            "error_count": result.error_count,
            "errors": errors_data,
            "warnings": result.warnings,
        },
        message="驗證通過" if result.is_valid else f"驗證失敗，共 {result.error_count} 個錯誤",
    )


# ======================================================================
# 5. 合併所有 Sheet
# ======================================================================

@router.get("/merge", response_model=ApiResponse)
async def merge_all_sheets() -> ApiResponse:
    """合併所有 Sheet 為一個資料表，自動加上 _source_sheet 欄位。"""
    if not SAMPLE_FILE.exists():
        raise HTTPException(status_code=404, detail="範例檔案不存在")

    handler = ExcelHandler(SAMPLE_FILE)
    merged = await _run_sync(handler.merge_sheets, add_source_column=True)
    records = merged.where(merged.notna(), None).to_dict(orient="records")

    return ApiResponse(
        data={
            "total": len(records),
            "columns": list(merged.columns),
            "rows": records,
        },
        message=f"合併完成，共 {len(records)} 行",
    )


# ======================================================================
# 6. 匯出下載（美化 Excel）
# ======================================================================

@router.get("/export", response_class=StreamingResponse)
async def export_excel(
    sheet_name: Optional[str] = Query(default=None, description="指定 Sheet，不填則合併全部"),
) -> StreamingResponse:
    """匯出美化後的 Excel 檔案供下載。"""
    if not SAMPLE_FILE.exists():
        raise HTTPException(status_code=404, detail="範例檔案不存在")

    handler = ExcelHandler(SAMPLE_FILE)

    if sheet_name:
        df = await _run_sync(handler.read_sheet, sheet_name=sheet_name)
        filename = f"{sheet_name}.xlsx"
    else:
        df = await _run_sync(handler.merge_sheets)
        filename = "merged_export.xlsx"

    buffer = await _run_sync(handler.write, df)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ======================================================================
# 7. 上傳 Excel 並讀取
# ======================================================================

@router.post("/upload", response_model=ApiResponse)
async def upload_excel(file: UploadFile = File(...)) -> ApiResponse:
    """
    上傳 Excel 檔案並回傳所有 Sheet 的資料摘要。

    支援 .xlsx 格式。
    """
    if not file.filename or not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="僅支援 .xlsx 格式")

    # 存到暫存目錄
    upload_dir = Path(__file__).resolve().parents[4] / "uploads"
    upload_dir.mkdir(exist_ok=True)

    # 使用安全檔名
    safe_name = Path(file.filename).name
    save_path = upload_dir / safe_name
    content = await file.read()
    save_path.write_bytes(content)

    try:
        handler = ExcelHandler(save_path)
        sheet_info = await _run_sync(handler.get_sheet_info)
        all_data: Dict[str, Any] = {"file": safe_name, "sheets": []}

        for info in sheet_info:
            df = await _run_sync(handler.read_sheet, sheet_name=info["name"])
            records = df.head(10).where(df.head(10).notna(), None).to_dict(orient="records")
            all_data["sheets"].append({
                "name": info["name"],
                "total_rows": len(df),
                "columns": list(df.columns),
                "preview": records,
            })

        return ApiResponse(data=all_data, message=f"上傳成功，共 {len(sheet_info)} 個工作表")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"解析失敗：{e}")
    finally:
        # 清理暫存檔
        if save_path.exists():
            save_path.unlink()


# ======================================================================
# 8. 統計摘要
# ======================================================================

@router.get("/sheets/{sheet_name}/stats", response_model=ApiResponse)
async def sheet_stats(sheet_name: str) -> ApiResponse:
    """取得指定 Sheet 的數值欄位統計（count, mean, min, max, sum）。"""
    if not SAMPLE_FILE.exists():
        raise HTTPException(status_code=404, detail="範例檔案不存在")

    handler = ExcelHandler(SAMPLE_FILE)
    try:
        df = await _run_sync(handler.read_sheet, sheet_name=sheet_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"讀取失敗：{e}")

    # 只對數值欄位做統計
    numeric_df = df.select_dtypes(include=["number"])
    if numeric_df.empty:
        return ApiResponse(data={}, message="該 Sheet 沒有數值欄位")

    stats: Dict[str, Any] = {}
    for col in numeric_df.columns:
        series = numeric_df[col].dropna()
        stats[col] = {
            "count": int(series.count()),
            "sum": float(series.sum()),
            "mean": round(float(series.mean()), 2),
            "min": float(series.min()),
            "max": float(series.max()),
            "median": float(series.median()),
        }

    return ApiResponse(data=stats, message=f"統計了 {len(stats)} 個數值欄位")

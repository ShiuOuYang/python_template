from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.v1.routes import analysis, board, excel, probe_result, test_run
from app.core.config import settings
from app.core.database import init_db
from app.schemas.base import ApiResponse


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """應用程式生命週期管理。"""
    # startup
    await init_db()
    yield
    # shutdown


app = FastAPI(
    title="Flying Probe Analysis System",
    description="飛針測試（Flying Probe）分析系統後端 API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS 設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.APP_ENV == "development" else [],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_model=ApiResponse)
async def root() -> ApiResponse:
    """根路徑健康檢查。"""
    return ApiResponse(data={"status": "ok"}, message="Flying Probe Analysis System is running")


# 註冊路由
app.include_router(board.router, prefix="/api/v1")
app.include_router(test_run.router, prefix="/api/v1")
app.include_router(probe_result.router, prefix="/api/v1")
app.include_router(analysis.router, prefix="/api/v1")
app.include_router(excel.router, prefix="/api/v1")


# ---------- 全局例外處理 ----------


@app.exception_handler(HTTPException)
async def http_exception_handler(
    request: Request, exc: HTTPException
) -> JSONResponse:
    """處理 HTTP 例外，回傳統一格式。"""
    return JSONResponse(
        status_code=exc.status_code,
        content=ApiResponse(
            success=False,
            message=str(exc.detail),
        ).model_dump(),
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """處理請求驗證錯誤，回傳統一格式。"""
    return JSONResponse(
        status_code=422,
        content=ApiResponse(
            success=False,
            data=exc.errors(),
            message="Validation error",
        ).model_dump(),
    )


@app.exception_handler(Exception)
async def global_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """處理未預期的例外，回傳統一格式。"""
    message = str(exc) if settings.APP_ENV == "development" else "Internal server error"
    return JSONResponse(
        status_code=500,
        content=ApiResponse(
            success=False,
            message=message,
        ).model_dump(),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host="localhost",
        port=settings.APP_PORT,
        reload=(settings.APP_ENV == "development"),
    )

"""Fstack HTTP application composition."""

from __future__ import annotations

import hmac
import os
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from fstack.provisioning import ProvisioningRequest, ProvisioningResponse
from fstack.service import (
    OrganizationExistsError,
    ProvisioningConflictError,
    PublicDomainExistsError,
    create_provisioning_operation,
)

_OPERATOR_PRINCIPAL = "installation-operator"
_BEARER = HTTPBearer(auto_error=False)


def _operator_token() -> str:
    token = os.environ.get("FSTACK_OPERATOR_TOKEN")
    if token is None or not token:
        raise RuntimeError("FSTACK_OPERATOR_TOKEN is required")
    return token


def _database_engine() -> Engine:
    database_url = os.environ.get("FSTACK_DATABASE_URL")
    if database_url is None:
        raise RuntimeError("FSTACK_DATABASE_URL is required")
    return create_engine(database_url, pool_pre_ping=True, hide_parameters=True)


def _require_operator(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_BEARER)],
) -> str:
    expected = _operator_token()
    if (
        credentials is None
        or credentials.scheme.lower() != "bearer"
        or not hmac.compare_digest(
            credentials.credentials.encode("utf-8"), expected.encode("utf-8")
        )
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "unauthorized", "message": "operator authorization required"},
        )
    return _OPERATOR_PRINCIPAL


def _validate_idempotency_key(value: str) -> str:
    if not 1 <= len(value) <= 200 or any(not 0x21 <= ord(character) <= 0x7E for character in value):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": "invalid_idempotency_key", "message": "invalid idempotency key"},
        )
    return value


def create_app() -> FastAPI:
    app = FastAPI(title="Fstack")
    engine = _database_engine()

    @app.exception_handler(ProvisioningConflictError)
    async def provisioning_conflict_handler(
        _request: Request,
        exc: ProvisioningConflictError,
    ) -> JSONResponse:
        messages = {
            "idempotency_conflict": "idempotency key was already used for a different request",
            "organization_exists": "organization slug is already reserved",
            "public_domain_exists": "public domain is already reserved",
        }
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"code": exc.code, "message": messages[exc.code]},
        )

    @app.exception_handler(StarletteHTTPException)
    async def stable_http_error_handler(
        _request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        if isinstance(exc.detail, dict) and set(exc.detail) == {"code", "message"}:
            return JSONResponse(status_code=exc.status_code, content=exc.detail)
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": "http_error", "message": "request failed"},
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        _request: Request,
        _exc: RequestValidationError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={"code": "invalid_request", "message": "request validation failed"},
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "fstack"}

    @app.post(
        "/v1/provisioning/operations",
        response_model=ProvisioningResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    def create_operation(
        request: ProvisioningRequest,
        response: Response,
        idempotency_key: str = Header(alias="Idempotency-Key"),
        actor_principal: str = Depends(_require_operator),
    ) -> ProvisioningResponse:
        exact_key = _validate_idempotency_key(idempotency_key)
        try:
            with engine.begin() as connection:
                result = create_provisioning_operation(
                    connection,
                    request=request,
                    idempotency_key=exact_key,
                    actor_principal=actor_principal,
                )
        except IntegrityError as exc:
            diagnostic = getattr(exc.orig, "diag", None)
            if getattr(diagnostic, "constraint_name", None) == "uq_organizations_slug":
                raise OrganizationExistsError from None
            if getattr(diagnostic, "constraint_name", None) == "uq_deployments_public_domain":
                raise PublicDomainExistsError from None
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "code": "persistence_failed",
                    "message": "request could not be persisted",
                },
            ) from None
        except SQLAlchemyError:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "code": "persistence_failed",
                    "message": "request could not be persisted",
                },
            ) from None
        if result.replayed:
            response.status_code = status.HTTP_200_OK
        return ProvisioningResponse(
            operation_id=result.operation_id,
            organization_id=result.organization_id,
            business_id=result.business_id,
            environment_id=result.environment_id,
            workload_id=result.workload_id,
            deployment_id=result.deployment_id,
            replayed=result.replayed,
        )

    return app

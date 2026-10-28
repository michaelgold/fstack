"""Fstack HTTP application composition."""

from __future__ import annotations

import hmac
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from fstack.billing import InvalidEntitlementRevision, parse_entitlement_revision_request
from fstack.billing_config import load_billing_configuration
from fstack.billing_service import (
    EntitlementRevisionConflict,
)
from fstack.billing_service import (
    synchronize_entitlement_revision as persist_entitlement_revision,
)
from fstack.billing_usage import InvalidUsageCursor, list_usage_facts, parse_usage_query
from fstack.identity import AuthConfiguration, mount_identity
from fstack.identity_mail import load_auth_configuration
from fstack.provisioning import (
    ProvisioningRequest,
    ProvisioningResponse,
    ProvisioningStatusResponse,
)
from fstack.service import (
    EntitlementQuotaExceededError,
    EntitlementStateUnavailableError,
    OrganizationExistsError,
    ProvisioningConflictError,
    PublicDomainExistsError,
    SubscriptionInactiveError,
    create_provisioning_operation,
    get_provisioning_operation,
    is_public_domain_authorized,
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


def _require_operator_for_status(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_BEARER)],
) -> str:
    try:
        return _require_operator(credentials)
    except HTTPException as exc:
        headers = dict(exc.headers or {})
        headers["Cache-Control"] = "no-store"
        raise HTTPException(
            status_code=exc.status_code,
            detail=exc.detail,
            headers=headers,
        ) from None


def _validate_idempotency_key(value: str) -> str:
    if not 1 <= len(value) <= 200 or any(not 0x21 <= ord(character) <= 0x7E for character in value):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": "invalid_idempotency_key", "message": "invalid idempotency key"},
        )
    return value


def create_app(*, auth_configuration: AuthConfiguration | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            engine.dispose()

    app = FastAPI(title="Fstack", lifespan=lifespan)
    billing_configuration = load_billing_configuration(
        operator_token=os.environ.get("FSTACK_OPERATOR_TOKEN")
    )
    engine = _database_engine()
    identity = auth_configuration or load_auth_configuration()
    if identity is not None:
        app.include_router(mount_identity(engine, identity))

    @app.middleware("http")
    async def identity_privacy(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if not request.url.path.startswith(("/v1/auth/", "/v1/agent/", "/auth/")):
            return await call_next(request)
        try:
            response = await call_next(request)
        except Exception:
            response = JSONResponse(status_code=503, content={"code": "authentication_unavailable"})
        response.headers["Cache-Control"] = "no-store"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'"
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    def require_billing_reconcile(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_BEARER)],
    ) -> None:
        if billing_configuration.mode == "disabled":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        expected = billing_configuration.reconcile_token
        try:
            matches = (
                credentials is not None
                and credentials.scheme.lower() == "bearer"
                and expected is not None
                and hmac.compare_digest(
                    credentials.credentials.encode("utf-8"), expected.encode("utf-8")
                )
            )
        except UnicodeEncodeError:
            matches = False
        if not matches:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={
                    "code": "authentication_required",
                    "message": "billing reconciliation authentication required",
                },
            )

    def require_billing_export(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_BEARER)],
    ) -> None:
        if billing_configuration.mode == "disabled":
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        expected = billing_configuration.export_token
        try:
            matches = (
                credentials is not None
                and credentials.scheme.lower() == "bearer"
                and expected is not None
                and hmac.compare_digest(
                    credentials.credentials.encode("utf-8"), expected.encode("utf-8")
                )
            )
        except UnicodeEncodeError:
            matches = False
        if not matches:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail={
                    "code": "authentication_required",
                    "message": "billing usage authentication required",
                },
            )

    @app.middleware("http")
    async def operation_status_no_store(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if request.method != "GET" or not request.url.path.startswith(
            "/v1/provisioning/operations/"
        ):
            return await call_next(request)
        try:
            response = await call_next(request)
        except Exception:
            response = JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"code": "internal_error", "message": "request failed"},
            )
        response.headers["Cache-Control"] = "no-store"
        return response

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

    @app.exception_handler(EntitlementStateUnavailableError)
    async def entitlement_state_unavailable_handler(
        _request: Request,
        _exc: EntitlementStateUnavailableError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "code": "entitlement_state_unavailable",
                "message": "entitlement state is unavailable",
            },
        )

    @app.exception_handler(SubscriptionInactiveError)
    async def subscription_inactive_handler(
        _request: Request,
        _exc: SubscriptionInactiveError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            content={
                "code": "subscription_inactive",
                "message": "subscription is inactive",
            },
        )

    @app.exception_handler(EntitlementQuotaExceededError)
    async def entitlement_quota_exceeded_handler(
        _request: Request,
        _exc: EntitlementQuotaExceededError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={
                "code": "entitlement_quota_exceeded",
                "message": "entitlement quota is exhausted",
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def stable_http_error_handler(
        _request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        if isinstance(exc.detail, dict) and set(exc.detail) == {"code", "message"}:
            return JSONResponse(
                status_code=exc.status_code,
                content=exc.detail,
                headers=exc.headers,
            )
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": "http_error", "message": "request failed"},
            headers=exc.headers,
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        request: Request,
        _exc: RequestValidationError,
    ) -> JSONResponse:
        headers = None
        if request.method == "GET" and request.url.path.startswith("/v1/provisioning/operations/"):
            headers = {"Cache-Control": "no-store"}
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            content={"code": "invalid_request", "message": "request validation failed"},
            headers=headers,
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "fstack"}

    @app.put("/internal/billing/entitlement-revisions", include_in_schema=False)
    async def synchronize_entitlement_revision(
        request: Request,
        response: Response,
        _authorized: None = Depends(require_billing_reconcile),
    ) -> dict[str, object]:
        try:
            if request.headers.get("content-type") != "application/json":
                raise InvalidEntitlementRevision
            revision = parse_entitlement_revision_request(await request.body())
        except InvalidEntitlementRevision:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "invalid_entitlement_revision",
                    "message": "entitlement revision is invalid",
                },
            ) from None
        try:
            with engine.begin() as connection:
                result = persist_entitlement_revision(connection, request=revision)
        except EntitlementRevisionConflict:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "code": "entitlement_revision_conflict",
                    "message": "entitlement revision conflicts with committed state",
                },
            ) from None
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "entitlement_persistence_unavailable",
                    "message": "entitlement revision could not be persisted",
                },
            ) from None
        response.status_code = status.HTTP_200_OK if result.replayed else status.HTTP_201_CREATED
        return {
            "entitlement_subject_id": str(result.entitlement_subject_id),
            "entitlement_revision_id": str(result.entitlement_revision_id),
            "replayed": result.replayed,
        }

    @app.get("/internal/billing/usage-facts", include_in_schema=False)
    def export_usage_facts(
        request: Request,
        _authorized: None = Depends(require_billing_export),
    ) -> dict[str, object]:
        try:
            raw_query = request.scope.get("query_string", b"")
            if not isinstance(raw_query, bytes) or len(raw_query) > 200:
                raise InvalidUsageCursor
            query = parse_usage_query(list(request.query_params.multi_items()))
        except InvalidUsageCursor:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "invalid_usage_cursor",
                    "message": "usage cursor is invalid",
                },
            ) from None
        try:
            with engine.connect() as connection:
                return list_usage_facts(connection, query=query)
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "usage_facts_unavailable",
                    "message": "usage facts are unavailable",
                },
            ) from None

    @app.get("/internal/caddy/ask", include_in_schema=False)
    def authorize_caddy_certificate(request: Request) -> Response:
        headers = {"Cache-Control": "no-store"}
        domains = request.query_params.getlist("domain")
        if len(domains) != 1:
            return Response(status_code=status.HTTP_400_BAD_REQUEST, headers=headers)
        domain = domains[0]
        try:
            ProvisioningRequest.validate_public_domain(domain)
        except ValueError:
            return Response(status_code=status.HTTP_400_BAD_REQUEST, headers=headers)
        try:
            with engine.connect() as connection:
                authorized = is_public_domain_authorized(connection, public_domain=domain)
        except Exception:
            return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, headers=headers)
        return Response(
            status_code=status.HTTP_200_OK if authorized else status.HTTP_403_FORBIDDEN,
            headers=headers,
        )

    @app.get(
        "/v1/provisioning/operations/{operation_id}",
        response_model=ProvisioningStatusResponse,
    )
    def get_operation(
        operation_id: UUID,
        response: Response,
        _actor_principal: str = Depends(_require_operator_for_status),
    ) -> ProvisioningStatusResponse:
        response.headers["Cache-Control"] = "no-store"
        try:
            with engine.connect() as connection:
                result = get_provisioning_operation(connection, operation_id=operation_id)
        except SQLAlchemyError:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={
                    "code": "persistence_failed",
                    "message": "request could not be persisted",
                },
                headers={"Cache-Control": "no-store"},
            ) from None
        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "code": "operation_not_found",
                    "message": "operation was not found",
                },
                headers={"Cache-Control": "no-store"},
            )
        return result

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
                    entitlement_mode=billing_configuration.mode,
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

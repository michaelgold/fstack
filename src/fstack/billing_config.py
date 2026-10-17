"""Installation-local billing configuration contracts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class BillingConfiguration:
    mode: Literal["disabled", "required"]
    reconcile_token: str | None
    export_token: str | None


def _valid_token(value: str | None) -> bool:
    if value is None or not 32 <= len(value) <= 200:
        return False
    return all(0x21 <= ord(character) <= 0x7E for character in value)


def load_billing_configuration(*, operator_token: str | None) -> BillingConfiguration:
    raw_mode = os.environ.get("FSTACK_ENTITLEMENT_MODE", "")
    if raw_mode in {"", "disabled"}:
        return BillingConfiguration(
            mode="disabled",
            reconcile_token=None,
            export_token=None,
        )
    if raw_mode != "required":
        raise RuntimeError("FSTACK_ENTITLEMENT_MODE is invalid")

    reconcile_token = os.environ.get("FSTACK_BILLING_RECONCILE_TOKEN")
    export_token = os.environ.get("FSTACK_BILLING_EXPORT_TOKEN")
    tokens = (reconcile_token, export_token)
    if (
        not all(_valid_token(token) for token in tokens)
        or reconcile_token == export_token
        or operator_token in tokens
    ):
        raise RuntimeError("billing token configuration is invalid")
    return BillingConfiguration(
        mode="required",
        reconcile_token=reconcile_token,
        export_token=export_token,
    )

import pytest
from sqlalchemy import Engine, inspect


@pytest.mark.postgres
def test_initial_migration_creates_provisioning_authority_tables(
    migrated_database: Engine,
) -> None:
    tables = set(inspect(migrated_database).get_table_names())

    assert {
        "organizations",
        "businesses",
        "environments",
        "workloads",
        "deployments",
        "database_bindings",
        "storage_bindings",
        "provisioning_operations",
        "audit_events",
    } <= tables

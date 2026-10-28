"""Installation-local human identity and bounded agent delegation."""

from alembic import op

revision = "20261028_0007"
down_revision = "20261017_0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE auth_principals (
        id uuid PRIMARY KEY, email text COLLATE "C" NOT NULL UNIQUE,
        active boolean NOT NULL DEFAULT true
    );
    CREATE TABLE auth_memberships (
        account_id uuid REFERENCES organizations(id),
        principal_id uuid REFERENCES auth_principals(id),
        owner boolean NOT NULL DEFAULT false,
        active boolean NOT NULL DEFAULT true,
        permissions text[] NOT NULL DEFAULT '{}',
        PRIMARY KEY (account_id, principal_id),
        CHECK (permissions <@ ARRAY['account:read']::text[])
    );
    CREATE TABLE auth_logins (
        digest text PRIMARY KEY, principal_id uuid NOT NULL REFERENCES auth_principals(id),
        binding text NOT NULL, expires_at timestamptz NOT NULL,
        consumed boolean NOT NULL DEFAULT false
    );
    CREATE TABLE auth_sessions (
        digest text PRIMARY KEY, principal_id uuid NOT NULL REFERENCES auth_principals(id),
        expires_at timestamptz NOT NULL, revoked boolean NOT NULL DEFAULT false
    );
    CREATE TABLE auth_devices (
        digest text PRIMARY KEY, user_digest text UNIQUE NOT NULL,
        account_id uuid NOT NULL REFERENCES organizations(id),
        client text NOT NULL CHECK (length(client) BETWEEN 1 AND 100),
        scopes text[] NOT NULL CHECK (scopes = ARRAY['account:read']::text[]),
        expires_at timestamptz NOT NULL,
        state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','approved','consumed')),
        principal_id uuid,
        FOREIGN KEY (account_id, principal_id)
            REFERENCES auth_memberships(account_id, principal_id),
        CHECK ((state = 'pending') = (principal_id IS NULL))
    );
    CREATE TABLE auth_grants (
        id uuid PRIMARY KEY, digest text UNIQUE NOT NULL,
        account_id uuid NOT NULL, principal_id uuid NOT NULL,
        client text NOT NULL, scopes text[] NOT NULL CHECK (scopes = ARRAY['account:read']::text[]),
        audience text NOT NULL CHECK (audience = 'fstack-agent'),
        expires_at timestamptz NOT NULL, revoked boolean NOT NULL DEFAULT false,
        FOREIGN KEY (account_id, principal_id) REFERENCES auth_memberships(account_id, principal_id)
    );
    CREATE FUNCTION auth_device_transition() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF ROW(NEW.digest,NEW.user_digest,NEW.account_id,NEW.client,NEW.scopes)
            IS DISTINCT FROM ROW(OLD.digest,OLD.user_digest,OLD.account_id,OLD.client,OLD.scopes)
            OR NEW.expires_at > OLD.expires_at
            OR NOT ((OLD.state='pending' AND NEW.state='approved')
                 OR (OLD.state='approved' AND NEW.state='consumed')
                 OR (OLD.state=NEW.state
                     AND NEW.principal_id IS NOT DISTINCT FROM OLD.principal_id))
            OR (OLD.principal_id IS NOT NULL AND NEW.principal_id IS DISTINCT FROM OLD.principal_id)
        THEN RAISE EXCEPTION 'invalid device transition' USING ERRCODE='23514'; END IF;
        RETURN NEW;
    END $$;
    CREATE TRIGGER auth_device_transition BEFORE UPDATE ON auth_devices
        FOR EACH ROW EXECUTE FUNCTION auth_device_transition();
    CREATE FUNCTION auth_grant_transition() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF ROW(NEW.id,NEW.digest,NEW.account_id,NEW.principal_id,NEW.client,NEW.scopes,NEW.audience)
            IS DISTINCT FROM
            ROW(OLD.id,OLD.digest,OLD.account_id,OLD.principal_id,OLD.client,OLD.scopes,OLD.audience)
            OR NEW.expires_at > OLD.expires_at OR (OLD.revoked AND NOT NEW.revoked)
        THEN RAISE EXCEPTION 'invalid grant transition' USING ERRCODE='23514'; END IF;
        RETURN NEW;
    END $$;
    CREATE TRIGGER auth_grant_transition BEFORE UPDATE ON auth_grants
        FOR EACH ROW EXECUTE FUNCTION auth_grant_transition();
    CREATE TABLE auth_rate_buckets (
        key text PRIMARY KEY, window_start timestamptz NOT NULL, count integer NOT NULL
    );
    """)


def downgrade() -> None:
    for name in (
        "auth_rate_buckets",
        "auth_grants",
        "auth_devices",
        "auth_sessions",
        "auth_logins",
        "auth_memberships",
        "auth_principals",
    ):
        op.drop_table(name)
    op.execute("DROP FUNCTION auth_grant_transition(); DROP FUNCTION auth_device_transition()")

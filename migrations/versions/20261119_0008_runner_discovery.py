"""Owner-enrolled user-managed runner observations (not execution authority)."""

from alembic import op

revision = "20261119_0008"
down_revision = "20261028_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE local_runners (
        id uuid PRIMARY KEY,
        account_id uuid NOT NULL,
        principal_id uuid NOT NULL,
        token_digest text UNIQUE NOT NULL CHECK (token_digest ~ '^[0-9a-f]{64}$'),
        machine_class text NOT NULL CHECK (machine_class ~ '^[a-z0-9][a-z0-9_.-]{0,63}$'),
        image text NOT NULL CHECK (image ~ '^[a-z0-9./_:-]+@sha256:[0-9a-f]{64}$'),
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        expires_at timestamptz NOT NULL,
        revoked boolean NOT NULL DEFAULT false,
        sequence bigint NOT NULL DEFAULT 0 CHECK (sequence >= 0),
        last_seen timestamptz,
        available_slots integer CHECK (available_slots BETWEEN 0 AND 64),
        queue_depth integer CHECK (queue_depth BETWEEN 0 AND 1000000),
        FOREIGN KEY (account_id,principal_id) REFERENCES auth_memberships(account_id,principal_id),
        CHECK (expires_at > created_at AND expires_at <= created_at + interval '1 day'),
        CHECK ((sequence=0 AND last_seen IS NULL
                AND available_slots IS NULL AND queue_depth IS NULL)
            OR (sequence>0 AND last_seen IS NOT NULL
                AND available_slots IS NOT NULL AND queue_depth IS NOT NULL))
    );
    CREATE FUNCTION local_runner_transition() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF ROW(NEW.id,NEW.account_id,NEW.principal_id,NEW.token_digest,
               NEW.machine_class,NEW.image,NEW.created_at,NEW.expires_at)
           IS DISTINCT FROM
           ROW(OLD.id,OLD.account_id,OLD.principal_id,OLD.token_digest,
               OLD.machine_class,OLD.image,OLD.created_at,OLD.expires_at)
           OR (OLD.revoked AND NOT NEW.revoked)
           OR NOT (
               (NEW.sequence=OLD.sequence AND
                ROW(NEW.last_seen,NEW.available_slots,NEW.queue_depth) IS NOT DISTINCT FROM
                ROW(OLD.last_seen,OLD.available_slots,OLD.queue_depth))
               OR (NOT OLD.revoked AND NOT NEW.revoked AND NEW.expires_at>clock_timestamp()
                   AND NEW.sequence>OLD.sequence AND NEW.last_seen IS NOT NULL
                   AND (OLD.last_seen IS NULL OR NEW.last_seen>=OLD.last_seen))
           )
        THEN RAISE EXCEPTION 'invalid runner transition' USING ERRCODE='23514'; END IF;
        RETURN NEW;
    END $$;
    CREATE TRIGGER local_runner_transition BEFORE UPDATE ON local_runners
        FOR EACH ROW EXECUTE FUNCTION local_runner_transition();
    CREATE INDEX local_runners_account ON local_runners(account_id);
    """)


def downgrade() -> None:
    op.drop_table("local_runners")
    op.execute("DROP FUNCTION local_runner_transition()")

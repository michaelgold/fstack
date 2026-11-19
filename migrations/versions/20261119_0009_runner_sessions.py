"""Single-runner explicit session grants, fenced immutable execution generation."""

from alembic import op

revision = "20261119_0009"
down_revision = "20261119_0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    ALTER TABLE local_runners ADD CONSTRAINT local_runner_scope UNIQUE (id,account_id,principal_id);
    CREATE TABLE local_sessions (
        id uuid PRIMARY KEY,
        account_id uuid NOT NULL,
        principal_id uuid NOT NULL,
        runner_id uuid NOT NULL,
        workspace_id uuid NOT NULL,
        request_digest text NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
        token_digest text NOT NULL UNIQUE CHECK (token_digest ~ '^[0-9a-f]{64}$'),
        client_public_key text NOT NULL CHECK
            (client_public_key ~ '^nodekey:[0-9a-f]{64}$'
             AND client_public_key <> 'nodekey:' || repeat('0',64)),
        image text NOT NULL,
        memory_mib integer NOT NULL CHECK (memory_mib BETWEEN 512 AND 4096),
        cpu_millis integer NOT NULL CHECK (cpu_millis BETWEEN 100 AND 2000),
        created_at timestamptz NOT NULL,
        expires_at timestamptz NOT NULL,
        state text NOT NULL DEFAULT 'requested'
            CHECK (state IN ('requested','leased','ready','stopping','stopped')),
        generation integer NOT NULL DEFAULT 0 CHECK (generation BETWEEN 0 AND 1),
        allocated_at timestamptz,
        stopped_at timestamptz,
        FOREIGN KEY (account_id,principal_id) REFERENCES auth_memberships(account_id,principal_id),
        FOREIGN KEY (runner_id,account_id,principal_id)
            REFERENCES local_runners(id,account_id,principal_id),
        CHECK (expires_at>created_at AND expires_at<=created_at+interval '5 minutes'),
        CHECK ((generation=0)=(allocated_at IS NULL)),
        CHECK ((state='stopped')=(stopped_at IS NOT NULL)),
        CHECK (state NOT IN ('leased','ready') OR generation=1),
        CHECK (state<>'requested' OR generation=0)
    );
    CREATE UNIQUE INDEX one_live_runner_session ON local_sessions(runner_id)
        WHERE state<>'stopped';
    CREATE FUNCTION local_session_transition() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF ROW(NEW.id,NEW.account_id,NEW.principal_id,NEW.runner_id,NEW.workspace_id,
               NEW.request_digest,NEW.token_digest,NEW.client_public_key,NEW.image,
               NEW.memory_mib,NEW.cpu_millis,NEW.created_at,NEW.expires_at)
           IS DISTINCT FROM ROW(OLD.id,OLD.account_id,OLD.principal_id,OLD.runner_id,
               OLD.workspace_id,OLD.request_digest,OLD.token_digest,OLD.client_public_key,
               OLD.image,OLD.memory_mib,OLD.cpu_millis,OLD.created_at,OLD.expires_at)
           OR NOT (
             (NEW.state=OLD.state AND NEW.generation=OLD.generation
                AND NEW.allocated_at IS NOT DISTINCT FROM OLD.allocated_at
                AND NEW.stopped_at IS NOT DISTINCT FROM OLD.stopped_at)
             OR (OLD.state='requested' AND NEW.state='leased' AND NEW.generation=1
                AND NEW.allocated_at IS NOT NULL AND NEW.expires_at>clock_timestamp())
             OR (OLD.state='leased' AND NEW.state='ready'
                AND NEW.generation=OLD.generation AND NEW.expires_at>clock_timestamp()
                AND NEW.allocated_at IS NOT DISTINCT FROM OLD.allocated_at)
             OR (OLD.state IN ('requested','leased','ready') AND NEW.state='stopping'
                AND NEW.generation=OLD.generation
                AND NEW.allocated_at IS NOT DISTINCT FROM OLD.allocated_at)
             OR (OLD.state='stopping' AND NEW.state='stopped'
                AND NEW.generation=OLD.generation AND NEW.stopped_at>=OLD.created_at
                AND NEW.allocated_at IS NOT DISTINCT FROM OLD.allocated_at)
           )
        THEN RAISE EXCEPTION 'invalid session transition' USING ERRCODE='23514'; END IF;
        RETURN NEW;
    END $$;
    CREATE TRIGGER local_session_transition BEFORE UPDATE ON local_sessions
        FOR EACH ROW EXECUTE FUNCTION local_session_transition();
    """)


def downgrade() -> None:
    op.execute("""DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM local_sessions) THEN
            RAISE EXCEPTION 'retained session history prevents downgrade';
        END IF;
    END $$;""")
    op.drop_table("local_sessions")
    op.execute("DROP FUNCTION local_session_transition()")
    op.drop_constraint("local_runner_scope", "local_runners")

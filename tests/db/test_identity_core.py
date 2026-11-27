"""PostgreSQL tests for passwordless sign-in (LM-052).

The unit tests in services/identity/tests use an in-memory repository; these
run the real repository against a fresh database to prove the guarantees that
only PostgreSQL can give: unique normalized emails, one active link per user,
exactly one winner when a link is used concurrently, persistent rate limits
shared by replicas, and an append-only audit trail.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import Engine, create_engine, delete, func, select, text, update
from sqlalchemy.exc import IntegrityError

from infra.scripts import seed as seeding
from services.identity.app.db.schema import (
    auth_events,
    login_tokens,
    rate_limit_counters,
    sessions,
    users,
)
from services.identity.app.errors import InvalidOrExpiredLink, RateLimited
from services.identity.app.repository import PostgresIdentityRepository
from services.identity.app.service import IdentityService
from services.identity.app.tokens import hash_secret
from services.identity.tests.fakes import START, FakeClock, RecordingMailer, make_settings

from .conftest import downgrade, upgrade

pytestmark = pytest.mark.database

IP = "198.51.100.7"


@pytest.fixture
def identity_db(database_url: str, engine: Engine) -> Engine:
    upgrade("identity", database_url)
    return engine


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def mailer() -> RecordingMailer:
    return RecordingMailer()


def make_service(engine: Engine, clock: FakeClock, mailer: RecordingMailer, **overrides):
    return IdentityService(
        PostgresIdentityRepository(engine), make_settings(**overrides), mailer, clock
    )


@pytest.fixture
def service(identity_db, clock, mailer) -> IdentityService:
    return make_service(identity_db, clock, mailer)


def run(engine: Engine, statement) -> None:
    with engine.begin() as connection:
        connection.execute(statement)


def assert_rejected(engine: Engine, statement) -> None:
    with pytest.raises(IntegrityError), engine.begin() as connection:
        connection.execute(statement)


def scalar(engine: Engine, statement):
    with engine.connect() as connection:
        return connection.scalar(statement)


def count(engine: Engine, table, *conditions) -> int:
    return scalar(engine, select(func.count()).select_from(table).where(*conditions))


def request_and_token(service: IdentityService, mailer: RecordingMailer, email: str) -> str:
    pending = service.request_login(email, IP)
    service.send_login_email(pending)
    return mailer.last_token()


# --- schema constraints ------------------------------------------------------


def new_user(email: str = "alice@example.com"):
    return users.insert().values(id=uuid4(), email=email)


def test_email_is_unique(identity_db):
    run(identity_db, new_user())

    assert_rejected(identity_db, new_user())


@pytest.mark.parametrize("email", ["Alice@example.com", " alice@example.com", "@example.com"])
def test_email_must_be_stored_normalized(identity_db, email):
    assert_rejected(identity_db, new_user(email))


def token_row(user_id, **overrides):
    values = dict(
        id=uuid4(),
        user_id=user_id,
        token_hash=uuid4().hex * 2,
        created_at=START,
        expires_at=START + timedelta(minutes=15),
    )
    return login_tokens.insert().values(**(values | overrides))


def test_one_active_token_per_user(identity_db):
    user_id = uuid4()
    run(identity_db, users.insert().values(id=user_id, email="alice@example.com"))
    run(identity_db, token_row(user_id))

    assert_rejected(identity_db, token_row(user_id))
    run(identity_db, token_row(user_id, revoked_at=START))
    run(identity_db, token_row(user_id, consumed_at=START))


def test_token_cannot_be_both_consumed_and_revoked_or_expire_before_creation(identity_db):
    user_id = uuid4()
    run(identity_db, users.insert().values(id=user_id, email="alice@example.com"))

    assert_rejected(identity_db, token_row(user_id, consumed_at=START, revoked_at=START))
    assert_rejected(identity_db, token_row(user_id, expires_at=START))


def test_session_idle_expiry_cannot_pass_absolute_expiry(identity_db):
    user_id = uuid4()
    run(identity_db, users.insert().values(id=user_id, email="alice@example.com"))

    assert_rejected(
        identity_db,
        sessions.insert().values(
            id=uuid4(),
            session_hash="a" * 64,
            user_id=user_id,
            created_at=START,
            last_seen_at=START,
            idle_expires_at=START + timedelta(hours=2),
            absolute_expires_at=START + timedelta(hours=1),
        ),
    )


def test_rate_limit_scope_is_constrained(identity_db):
    assert_rejected(
        identity_db,
        rate_limit_counters.insert().values(
            scope="anything", key_hash="a" * 64, window_start=START, count=1
        ),
    )


@pytest.mark.parametrize(
    "statement",
    [
        update(auth_events).values(event_type="logout"),
        delete(auth_events),
        text("TRUNCATE auth_events"),
    ],
    ids=["update", "delete", "truncate"],
)
def test_auth_events_are_append_only(identity_db, statement):
    run(identity_db, auth_events.insert().values(event_type="login_requested"))

    assert_rejected(identity_db, statement)


def test_downgrade_removes_the_auth_events_trigger_function(database_url, engine):
    upgrade("identity", database_url)
    downgrade("identity", database_url)

    remaining = scalar(
        engine, text("SELECT count(*) FROM pg_proc WHERE proname = 'auth_events_reject_change'")
    )
    assert remaining == 0


def test_baseline_empties_identity(database_url, engine, monkeypatch, clock, mailer):
    # db-baseline wipes every service database; identity has no seed data.
    upgrade("identity", database_url)
    request_and_token(make_service(engine, clock, mailer), mailer, "alice@example.com")
    monkeypatch.setenv("LEDGERMESH_ENV", "test")

    seeding.baseline("identity", database_url)

    assert count(engine, users) == 0
    assert count(engine, auth_events) == 0


# --- sign-in flow against PostgreSQL ----------------------------------------


def test_case_variants_create_one_user(service, mailer, identity_db):
    request_and_token(service, mailer, "Alice@Example.com")
    request_and_token(service, mailer, "  alice@EXAMPLE.com")

    assert count(identity_db, users) == 1
    assert scalar(identity_db, select(users.c.email)) == "alice@example.com"


def test_only_the_token_hash_is_stored(service, mailer, identity_db):
    token = request_and_token(service, mailer, "alice@example.com")

    stored = scalar(identity_db, select(login_tokens.c.token_hash))
    assert stored == hash_secret(token)
    dump = scalar(
        identity_db,
        text(
            "SELECT string_agg(t::text, ' ') FROM ("
            "SELECT row_to_json(x)::text AS t FROM login_tokens x "
            "UNION ALL SELECT row_to_json(y)::text FROM auth_events y) z"
        ),
    )
    assert token not in dump
    assert "alice@example.com" not in dump


def test_full_sign_in_session_and_logout(service, mailer, identity_db, clock):
    token = request_and_token(service, mailer, "alice@example.com")

    signed_in = service.verify(token, IP, None)

    assert signed_in.session.user.email_verified_at == START
    assert service.resolve(signed_in.session_secret).kind == "user"
    assert scalar(identity_db, select(sessions.c.session_hash)) == hash_secret(
        signed_in.session_secret
    )

    service.logout(signed_in.session_secret, IP)

    assert service.resolve(signed_in.session_secret).kind == "guest"
    assert count(identity_db, sessions, sessions.c.revoked_at.is_not(None)) == 1
    types = [row.event_type for row in service.list_events(limit=10, before_id=None)]
    assert types == ["logout", "login_succeeded", "login_requested"]
    assert scalar(identity_db, select(auth_events.c.client_ip).limit(1)) is not None


def test_reused_and_expired_links_fail(service, mailer, clock):
    used = request_and_token(service, mailer, "alice@example.com")
    service.verify(used, IP, None)
    expired = request_and_token(service, mailer, "bob@example.com")
    clock.advance(901)

    for token in (used, expired):
        with pytest.raises(InvalidOrExpiredLink):
            service.verify(token, IP, None)

    reasons = [e.reason for e in service.list_events(limit=10, before_id=None) if e.reason]
    assert reasons[:2] == ["expired", "used"]


def test_new_request_revokes_the_old_link(service, mailer, identity_db):
    first = request_and_token(service, mailer, "alice@example.com")
    second = request_and_token(service, mailer, "alice@example.com")

    with pytest.raises(InvalidOrExpiredLink):
        service.verify(first, IP, None)
    service.verify(second, IP, None)
    assert count(identity_db, login_tokens, login_tokens.c.revoked_at.is_not(None)) == 1


def test_session_idle_and_absolute_expiry(service, mailer, clock):
    token = request_and_token(service, mailer, "alice@example.com")
    secret = service.verify(token, IP, None).session_secret

    for _ in range(35):
        clock.advance(1200)
        assert service.resolve(secret).kind == "user"
    clock.advance(1200)
    assert service.resolve(secret).kind == "guest"

    other = service.verify(request_and_token(service, mailer, "bob@example.com"), IP, None)
    clock.advance(1800)
    assert service.resolve(other.session_secret).kind == "guest"


def test_concurrent_use_of_one_link_has_exactly_one_winner(service, mailer, identity_db):
    token = request_and_token(service, mailer, "alice@example.com")
    barrier = Barrier(10)

    def attempt(_):
        barrier.wait()
        try:
            service.verify(token, IP, None)
        except InvalidOrExpiredLink:
            return "rejected"
        return "signed-in"

    with ThreadPoolExecutor(max_workers=10) as pool:
        outcomes = list(pool.map(attempt, range(10)))

    assert outcomes.count("signed-in") == 1
    assert count(identity_db, sessions) == 1
    assert count(identity_db, auth_events, auth_events.c.event_type == "login_failed") == 9


def test_concurrent_requests_for_a_new_address_create_one_user_and_one_active_link(
    identity_db, clock, mailer
):
    service = make_service(identity_db, clock, mailer, login_limit_per_email=100)
    barrier = Barrier(10)

    def attempt(_):
        barrier.wait()
        service.request_login("new@example.com", IP)

    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(attempt, range(10)))

    assert count(identity_db, users) == 1
    assert count(identity_db, login_tokens) == 10
    active = login_tokens.c.consumed_at.is_(None) & login_tokens.c.revoked_at.is_(None)
    assert count(identity_db, login_tokens, active) == 1


# --- persistent rate limits ---------------------------------------------------


def test_rate_limit_is_shared_by_replicas_and_survives_restarts(
    database_url, identity_db, clock, mailer
):
    second_engine = create_engine(database_url)
    try:
        replica_a = make_service(identity_db, clock, mailer)
        replica_b = make_service(second_engine, clock, mailer)
        for index in range(5):
            (replica_a if index % 2 else replica_b).request_login("alice@example.com", IP)

        with pytest.raises(RateLimited) as limited:
            replica_a.request_login("alice@example.com", IP)
    finally:
        second_engine.dispose()

    assert 1 <= limited.value.retry_after <= 900
    # A "restarted" service with a new engine still sees the counter.
    restarted = make_service(create_engine(database_url), clock, mailer)
    with pytest.raises(RateLimited):
        restarted.request_login("alice@example.com", IP)
    restarted.repository.engine.dispose()
    masked = scalar(
        identity_db,
        select(auth_events.c.email_masked).where(auth_events.c.event_type == "rate_limited"),
    )
    assert masked == "a***@example.com"


def test_rate_limit_window_rolls_over_and_old_counters_are_removed(service, clock, identity_db):
    for _ in range(5):
        service.request_login("alice@example.com", IP)
    with pytest.raises(RateLimited):
        service.request_login("alice@example.com", IP)

    clock.advance(900)
    service.request_login("alice@example.com", IP)
    clock.advance(2 * 24 * 3600)
    service.request_login("alice@example.com", IP)

    # Counters older than a day were removed by the later attempt.
    assert scalar(identity_db, select(func.min(rate_limit_counters.c.window_start))) > START


def test_verify_attempts_are_limited_per_ip(service):
    for _ in range(30):
        with pytest.raises(InvalidOrExpiredLink):
            service.verify("q" * 43, IP, None)

    with pytest.raises(RateLimited):
        service.verify("q" * 43, IP, None)
    with pytest.raises(InvalidOrExpiredLink):
        service.verify("q" * 43, "203.0.113.9", None)

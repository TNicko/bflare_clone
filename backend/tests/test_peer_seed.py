"""Peer-seed queue rules. Inserting a peer does not start a harvest job."""

from db.crud.system.peer_seed import (
    queue_status_action,
    should_start_peer_seed_job,
)


def test_queue_insert_does_not_start_a_job() -> None:
    assert queue_status_action(None) == "insert"
    assert queue_status_action("failed") == "requeue"
    assert queue_status_action("pending") is None
    assert queue_status_action("running") is None
    assert queue_status_action("succeeded") is None
    assert not should_start_peer_seed_job(job_active=False, has_pending=True)
    assert not should_start_peer_seed_job(job_active=True, has_pending=True)
    assert not should_start_peer_seed_job(job_active=False, has_pending=False)

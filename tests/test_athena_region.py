"""Regression tests: every Athena/S3 call in util.athena must be pinned to us-west-2.

The riverscapes-athena-output bucket and the Athena workgroup live in us-west-2.
If awswrangler or a bare boto3.client() falls back to the shell/SSO session's region
(often us-east-1), Athena rejects the output location with:
  "The S3 location provided to save your query results is invalid ... is in the same region"

These tests are offline: boto3.Session construction is mocked so no credentials,
network, or ~/.aws access is needed.
"""

import util.athena.athena as athena


def _fake_session_factory(monkeypatch) -> list:
    """Replace boto3.Session with a fake that records created instances."""
    instances: list = []

    class FakeSession:  # noqa: D401 - simple stand-in for boto3.Session
        def __init__(self, *args, **kwargs):
            instances.append(self)

        def client(self, *args, **kwargs):
            return object()

    monkeypatch.setattr(athena.boto3, "Session", FakeSession)
    monkeypatch.setattr(athena, "_boto3_session", None)
    return instances


def test_get_boto3_session_is_pinned_to_us_west_2(monkeypatch):
    """The session must always be created with region_name=us-west-2, even when the
    environment resolves to a different region (e.g. an SSO profile defaulting to us-east-1)."""
    seen_regions: list = []

    class FakeSession:
        def __init__(self, *args, **kwargs):
            seen_regions.append(kwargs.get("region_name"))

        def client(self, *args, **kwargs):
            return object()

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setattr(athena.boto3, "Session", FakeSession)
    monkeypatch.setattr(athena, "_boto3_session", None)

    athena.get_boto3_session()

    assert seen_regions == ["us-west-2"]


def test_bare_clients_come_from_pinned_session(monkeypatch):
    """boto3.client('athena') / boto3.client('s3') helpers must use the pinned session."""
    instances = _fake_session_factory(monkeypatch)

    # These paths call get_boto3_session().client(...) - they must not raise and
    # must produce the single cached session instance.
    athena.get_boto3_session().client("athena")
    athena.get_boto3_session().client("s3")

    assert len(instances) == 1  # session is created once and cached


def test_wrangler_calls_receive_pinned_session(monkeypatch, tmp_path):
    """Every awswrangler call in the UNLOAD/download path must receive the pinned
    session as boto3_session, never defaulting to the ambient session."""
    instances = _fake_session_factory(monkeypatch)
    seen: list = []

    def fake_unload(*args, **kwargs):
        seen.append(("unload", kwargs.get("boto3_session")))
        return None

    def fake_list_objects(*args, **kwargs):
        seen.append(("list", kwargs.get("boto3_session")))
        return ["s3://riverscapes-athena-output/athena_unload/unit-test/0000-0.parquet"]

    def fake_download(*args, **kwargs):
        seen.append(("download", kwargs.get("boto3_session")))

    monkeypatch.setattr(athena.wr.athena, "unload", fake_unload)
    monkeypatch.setattr(athena.wr.s3, "list_objects", fake_list_objects)
    monkeypatch.setattr(athena.wr.s3, "download", fake_download)

    athena.query_to_local_parquet("SELECT 1", tmp_path)

    assert len(seen) == 3
    for name, sess in seen:
        assert sess is not None, f"wr.{name} called without a boto3_session"
        assert sess is instances[0], f"wr.{name} did not receive the pinned session"
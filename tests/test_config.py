

class TestIngestGatewayRouting:
    """Telemetry goes to the collector gateway; the API host is unchanged."""

    def test_default_policy_sends_telemetry_to_the_gateway(self):
        from meshai_cc.config import Policy

        p = Policy()
        assert p.resolved_ingest_url() == "https://ingest.meshai.dev"
        assert p.base_url == "https://api.meshai.dev"  # rates/heartbeat unchanged

    def test_explicit_ingest_url_wins(self):
        from meshai_cc.config import Policy

        assert Policy(ingest_url="https://custom.example").resolved_ingest_url() == "https://custom.example"

    def test_self_hosted_base_url_keeps_its_own_telemetry(self):
        """Regression guard: a self-hoster's spans must not be shipped to
        MeshAI's gateway just because they left ingest_url unset."""
        from meshai_cc.config import Policy

        p = Policy(base_url="https://meshai.internal.corp")
        assert p.resolved_ingest_url() == "https://meshai.internal.corp"

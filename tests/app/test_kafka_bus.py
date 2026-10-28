"""KafkaBus client kwargs: security modes map to aiokafka's protocol/mechanism and SSL needs a context."""

import ssl

from bus.kafka import KafkaBus
from config import Settings


def _kwargs(**kw):
    return KafkaBus(Settings(mode="databricks", kafka_username="u", kafka_password="p", **kw))._sasl_kwargs()


def test_scram_mode_for_redpanda():
    kw = _kwargs(kafka_security="SASL_SSL_SCRAM_SHA_256")
    assert kw["security_protocol"] == "SASL_SSL"
    assert kw["sasl_mechanism"] == "SCRAM-SHA-256"
    assert isinstance(kw["ssl_context"], ssl.SSLContext)
    assert (kw["sasl_plain_username"], kw["sasl_plain_password"]) == ("u", "p")


def test_plain_mode_and_raw_fallback():
    assert _kwargs(kafka_security="SASL_SSL_PLAIN")["sasl_mechanism"] == "PLAIN"
    raw = _kwargs(kafka_security_protocol="PLAINTEXT")
    assert raw == {"security_protocol": "PLAINTEXT"}

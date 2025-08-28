
import json
import pytest
from gs_client import read_service_account_secret, _normalize_private_key

VALID_INFO = {
    "type": "service_account",
    "project_id": "demo",
    "private_key_id": "abc",
    "private_key": "-----BEGIN PRIVATE KEY-----\nLINE\n-----END PRIVATE KEY-----\n",
    "client_email": "sa@demo.iam.gserviceaccount.com",
    "client_id": "123",
    "token_uri": "https://oauth2.googleapis.com/token"
}

def test_read_secret_from_mapping_ok():
    secrets = {"google_service_account": dict(VALID_INFO)}
    info = read_service_account_secret(secrets=secrets)
    assert isinstance(info, dict)
    assert info["client_email"].startswith("sa@")

def test_read_secret_from_json_ok():
    j = json.dumps({**VALID_INFO, "private_key": "-----BEGIN PRIVATE KEY-----\\nLINE\\n-----END PRIVATE KEY-----\\n"})
    secrets = {"google_service_account": j}
    info = read_service_account_secret(secrets=secrets)
    assert "\n" in info["private_key"]  # normalized

def test_missing_key():
    with pytest.raises(ValueError):
        read_service_account_secret(secrets={})

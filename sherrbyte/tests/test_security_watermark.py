from app.security import (
    decode_token, hash_password, make_access_token, make_refresh_token, verify_password,
)
from app.sherr import watermark
from app.sherr.prompts import build_user_prompt, normalize_mode


def test_password_hash_roundtrip():
    h = hash_password("hunter2")
    assert verify_password("hunter2", h)
    assert not verify_password("wrong", h)


def test_access_token_roundtrip_and_kind():
    tok = make_access_token("user-123")
    assert decode_token(tok, expected_kind="access") == "user-123"
    # An access token must not validate as a refresh token.
    assert decode_token(tok, expected_kind="refresh") is None


def test_refresh_token_roundtrip():
    tok = make_refresh_token("user-9")
    assert decode_token(tok, expected_kind="refresh") == "user-9"


def test_tampered_token_rejected():
    tok = make_access_token("user-1")
    assert decode_token(tok[:-2] + "00") is None


def test_watermark_apply_and_verify():
    env = watermark.apply("Generated text.", mode="BRIEF", model="gemini-2.5-flash")
    assert env["sgi"]["label"]
    assert watermark.verify(env)
    env["content"] = "tampered"
    assert not watermark.verify(env)


def test_normalize_mode_defaults_to_brief():
    assert normalize_mode("deep") == "DEEP"
    assert normalize_mode("nonsense") == "BRIEF"
    assert normalize_mode(None) == "BRIEF"


def test_build_user_prompt_includes_context():
    p = build_user_prompt("H", "S", "B", {"who": "X"}, ["src: detail"])
    assert "WHO: X" in p
    assert "src: detail" in p

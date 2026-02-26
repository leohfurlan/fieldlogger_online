from datetime import datetime, timedelta

from batch_analysis import build_signature_payload, resample_profile_101


def _sample_readings(temp_offset: float = 0.0):
    t0 = datetime(2026, 2, 25, 10, 0, 0)
    return [
        {
            "ts": t0,
            "temp": 100.0 + temp_offset,
            "corrente": 10.0,
            "start_signal": 0,
            "lid_signal": 1,
        },
        {
            "ts": t0 + timedelta(seconds=50),
            "temp": 200.0 + temp_offset,
            "corrente": 20.0,
            "start_signal": 1,
            "lid_signal": 1,
        },
        {
            "ts": t0 + timedelta(seconds=100),
            "temp": 300.0 + temp_offset,
            "corrente": 30.0,
            "start_signal": 1,
            "lid_signal": 0,
        },
    ]


def test_resample_profile_returns_101_points_and_interpolation():
    profile = resample_profile_101(_sample_readings())

    assert len(profile["time_pct"]) == 101
    assert len(profile["temp"]) == 101
    assert len(profile["corrente"]) == 101

    assert profile["temp"][0] == 100.0
    assert profile["temp"][-1] == 300.0
    assert profile["corrente"][0] == 10.0
    assert profile["corrente"][-1] == 30.0

    assert profile["temp"][50] == 200.0
    assert profile["corrente"][50] == 20.0


def test_signature_same_series_equal_and_small_variation_changes_hash():
    sig_a = build_signature_payload(_sample_readings())["signature"]
    sig_b = build_signature_payload(_sample_readings())["signature"]
    sig_c = build_signature_payload(_sample_readings(temp_offset=0.2))["signature"]

    assert sig_a == sig_b
    assert sig_a != sig_c

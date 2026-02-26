from datetime import datetime, timedelta

import pytest

import batch_analysis
from batch_analysis import (
    build_signature_payload,
    compute_batch_metrics,
    derive_batch_events,
    resample_profile_101,
)


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


def test_compute_batch_metrics_energy_proxy_mode(monkeypatch):
    monkeypatch.delenv("ENERGY_MODE", raising=False)
    monkeypatch.delenv("ENERGY_VOLTAGE_V", raising=False)
    monkeypatch.delenv("ENERGY_POWER_FACTOR", raising=False)
    batch_analysis._resolve_energy_config.cache_clear()

    readings = _sample_readings()
    events = derive_batch_events(readings)
    metrics = compute_batch_metrics(readings, events=events, batch_meta={})

    assert metrics["energy_mode"] == "proxy"
    assert metrics["energy_proxy"] == metrics["current_auc"]
    assert metrics["energy_estimated_kwh"] is None


def test_compute_batch_metrics_single_phase_energy_estimation(monkeypatch):
    monkeypatch.setenv("ENERGY_MODE", "single_phase")
    monkeypatch.setenv("ENERGY_VOLTAGE_V", "220")
    monkeypatch.setenv("ENERGY_POWER_FACTOR", "0.92")
    batch_analysis._resolve_energy_config.cache_clear()

    readings = _sample_readings()
    events = derive_batch_events(readings)
    metrics = compute_batch_metrics(readings, events=events, batch_meta={})

    assert metrics["energy_mode"] == "single_phase"
    assert metrics["current_auc"] == pytest.approx(2000.0, rel=1e-9)
    assert metrics["energy_estimated_wh"] == pytest.approx(112.444444, rel=1e-6)
    assert metrics["energy_estimated_kwh"] == pytest.approx(0.112444444, rel=1e-9)

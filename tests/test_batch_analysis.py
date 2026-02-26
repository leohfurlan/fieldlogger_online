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


def test_compute_batch_metrics_energy_estimation_defaults(monkeypatch):
    monkeypatch.delenv("ENERGY_EST_ENABLED", raising=False)
    monkeypatch.delenv("ENERGY_VLL_VOLTS", raising=False)
    monkeypatch.delenv("ENERGY_VOLTAGE_V", raising=False)
    monkeypatch.delenv("ENERGY_POWER_FACTOR", raising=False)
    monkeypatch.delenv("ENERGY_LABEL", raising=False)
    batch_analysis._resolve_energy_config.cache_clear()

    readings = _sample_readings()
    events = derive_batch_events(readings)
    metrics = compute_batch_metrics(readings, events=events, batch_meta={})

    assert metrics["energy_est_enabled"] is True
    assert metrics["vll_volts"] == pytest.approx(380.0, rel=1e-9)
    assert metrics["power_factor_assumed"] == pytest.approx(0.9, rel=1e-9)
    assert metrics["energy_label"] == "Energia estimada (kWh) — PF assumido"
    assert metrics["energy_proxy"] == metrics["current_auc"]
    assert metrics["current_auc"] == pytest.approx(2000.0, rel=1e-9)
    assert metrics["energy_est_kwh"] == pytest.approx(0.329089653, rel=1e-9)
    assert metrics["power_est_avg_kw"] == pytest.approx(23.694455, rel=1e-6)
    assert metrics["power_est_peak_kw"] == pytest.approx(17.770841, rel=1e-6)
    assert metrics["energy_estimated_kwh"] == metrics["energy_est_kwh"]


def test_compute_batch_metrics_energy_estimation_can_be_disabled(monkeypatch):
    monkeypatch.setenv("ENERGY_EST_ENABLED", "false")
    monkeypatch.setenv("ENERGY_VLL_VOLTS", "380")
    monkeypatch.setenv("ENERGY_POWER_FACTOR", "0.9")
    batch_analysis._resolve_energy_config.cache_clear()

    readings = _sample_readings()
    events = derive_batch_events(readings)
    metrics = compute_batch_metrics(readings, events=events, batch_meta={})

    assert metrics["energy_est_enabled"] is False
    assert metrics["current_auc"] == pytest.approx(2000.0, rel=1e-9)
    assert metrics["energy_proxy"] == pytest.approx(2000.0, rel=1e-9)
    assert metrics["energy_est_kwh"] is None
    assert metrics["power_est_avg_kw"] is None
    assert metrics["power_est_peak_kw"] is None


def test_compute_batch_metrics_energy_estimation_changes_with_power_factor(monkeypatch):
    monkeypatch.setenv("ENERGY_EST_ENABLED", "true")
    monkeypatch.setenv("ENERGY_VLL_VOLTS", "380")
    monkeypatch.setenv("ENERGY_POWER_FACTOR", "1.0")
    batch_analysis._resolve_energy_config.cache_clear()

    readings = _sample_readings()
    events = derive_batch_events(readings)
    metrics_pf_100 = compute_batch_metrics(readings, events=events, batch_meta={})

    monkeypatch.setenv("ENERGY_POWER_FACTOR", "0.5")
    batch_analysis._resolve_energy_config.cache_clear()
    metrics_pf_050 = compute_batch_metrics(readings, events=events, batch_meta={})

    assert metrics_pf_100["energy_est_kwh"] == pytest.approx(0.36565517, rel=1e-8)
    assert metrics_pf_050["energy_est_kwh"] == pytest.approx(0.182827585, rel=1e-8)
    assert metrics_pf_050["energy_est_kwh"] == pytest.approx(
        metrics_pf_100["energy_est_kwh"] * 0.5,
        rel=1e-8,
    )

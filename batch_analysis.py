from __future__ import annotations

import hashlib
import math
import os
from bisect import bisect_right
from datetime import datetime
from functools import lru_cache
from typing import Any

DEFAULT_ENERGY_EST_ENABLED = True
DEFAULT_ENERGY_VLL_VOLTS = 380.0
DEFAULT_ENERGY_POWER_FACTOR = 0.90
DEFAULT_ENERGY_LABEL = "Energia estimada (kWh) — PF assumido"
DEFAULT_DURATION_RECONCILE_TOLERANCE_S = 60.0


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isfinite(out):
        return out
    return None


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _as_bit(value: Any) -> int:
    try:
        return 1 if int(value) > 0 else 0
    except Exception:
        return 0


def _round_or_none(value: float | None, places: int = 4) -> float | None:
    if value is None:
        return None
    return round(float(value), places)


def _avg(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / float(len(values))


@lru_cache(maxsize=1)
def _resolve_energy_config() -> dict:
    enabled = _to_bool(
        os.getenv("ENERGY_EST_ENABLED"),
        default=DEFAULT_ENERGY_EST_ENABLED,
    )

    vll_volts = _to_float(os.getenv("ENERGY_VLL_VOLTS"))
    if vll_volts is None:
        # Compatibilidade com chave antiga.
        vll_volts = _to_float(os.getenv("ENERGY_VOLTAGE_V"))
    if vll_volts is None or vll_volts <= 0:
        vll_volts = float(DEFAULT_ENERGY_VLL_VOLTS)

    power_factor = _to_float(os.getenv("ENERGY_POWER_FACTOR"))
    if power_factor is None or power_factor <= 0 or power_factor > 1:
        power_factor = float(DEFAULT_ENERGY_POWER_FACTOR)

    label = str(os.getenv("ENERGY_LABEL", DEFAULT_ENERGY_LABEL) or "").strip()
    if not label:
        label = DEFAULT_ENERGY_LABEL

    return {
        "enabled": enabled,
        "vll_volts": vll_volts,
        "power_factor": power_factor,
        "label": label,
    }


def _compute_auc_points(points: list[tuple[datetime, float]]) -> float | None:
    auc = 0.0
    segments = 0

    for (left_ts, left_v), (right_ts, right_v) in zip(points, points[1:]):
        delta_s = (right_ts - left_ts).total_seconds()
        if delta_s <= 0:
            continue
        auc += ((left_v + right_v) / 2.0) * delta_s
        segments += 1

    if segments <= 0:
        return None
    return auc


def _build_energy_metrics(readings: list[dict], duration_s: float | None) -> dict:
    config = _resolve_energy_config()
    enabled = bool(config.get("enabled"))
    vll_volts = _to_float(config.get("vll_volts"))
    power_factor = _to_float(config.get("power_factor"))
    energy_label = str(config.get("label") or DEFAULT_ENERGY_LABEL)

    out = {
        "energy_est_enabled": enabled,
        "energy_label": energy_label,
        "vll_volts": _round_or_none(vll_volts, 3),
        "power_factor_assumed": _round_or_none(power_factor, 4),
        "energy_est_wh": None,
        "energy_est_kwh": None,
        "power_est_avg_kw": None,
        "power_est_peak_kw": None,
        # Chaves antigas mantidas por compatibilidade.
        "energy_estimated_wh": None,
        "energy_estimated_kwh": None,
    }

    if not enabled or vll_volts is None or power_factor is None:
        return out

    power_points: list[tuple[datetime, float]] = []
    for row in readings:
        ts = row.get("ts")
        if not isinstance(ts, datetime):
            continue
        corrente = _to_float(row.get("corrente"))
        if corrente is None:
            continue
        power_est_w = math.sqrt(3.0) * vll_volts * corrente * power_factor
        power_points.append((ts, power_est_w))

    if not power_points:
        return out

    power_values = [value for _, value in power_points]
    power_auc_ws = _compute_auc_points(power_points)
    avg_power_w = _avg(power_values)

    if power_auc_ws is not None:
        energy_wh = power_auc_ws / 3600.0
        energy_kwh = energy_wh / 1000.0
        out["energy_est_wh"] = _round_or_none(energy_wh, 6)
        out["energy_est_kwh"] = _round_or_none(energy_kwh, 9)

        if duration_s is not None and duration_s > 0:
            avg_power_w = power_auc_ws / duration_s

    peak_power_w = max(power_values)
    out["power_est_avg_kw"] = _round_or_none(
        None if avg_power_w is None else (avg_power_w / 1000.0),
        6,
    )
    out["power_est_peak_kw"] = _round_or_none(peak_power_w / 1000.0, 6)

    out["energy_estimated_wh"] = out["energy_est_wh"]
    out["energy_estimated_kwh"] = out["energy_est_kwh"]
    return out


def _event_priority(event_type: str) -> int:
    return {
        "START": 0,
        "LID_CLOSE": 1,
        "LID_OPEN": 2,
        "END": 3,
    }.get(event_type, 9)


def derive_batch_events(readings: list[dict], batch_meta: dict | None = None) -> list[dict]:
    ordered = [row for row in readings if isinstance(row.get("ts"), datetime)]
    ordered.sort(key=lambda row: row["ts"])

    events: list[dict] = []
    seen: set[tuple[str, datetime]] = set()

    def add_event(ts: datetime | None, event_type: str, value: Any, reason: str) -> None:
        if not isinstance(ts, datetime):
            return
        key = (event_type, ts)
        if key in seen:
            return
        seen.add(key)
        events.append(
            {
                "ts": ts,
                "type": event_type,
                "value": value,
                "reason": reason,
            }
        )

    prev_start: int | None = None
    prev_lid: int | None = None

    for row in ordered:
        start_signal = _as_bit(row.get("start_signal"))
        lid_signal = _as_bit(row.get("lid_signal"))
        ts = row["ts"]

        if prev_start is None:
            if start_signal == 1:
                add_event(ts, "START", 1, "initial_start_high")
        elif prev_start == 0 and start_signal == 1:
            add_event(ts, "START", 1, "start_rising_edge")

        if prev_lid is None:
            if lid_signal == 0:
                add_event(ts, "LID_OPEN", 0, "initial_lid_open")
        elif prev_lid == 1 and lid_signal == 0:
            add_event(ts, "LID_OPEN", 0, "lid_falling_edge")
        elif prev_lid == 0 and lid_signal == 1:
            add_event(ts, "LID_CLOSE", 1, "lid_rising_edge")

        prev_start = start_signal
        prev_lid = lid_signal

    has_start = any(evt["type"] == "START" for evt in events)
    if not has_start:
        meta_start = batch_meta.get("start_ts") if isinstance(batch_meta, dict) else None
        if isinstance(meta_start, datetime):
            add_event(meta_start, "START", 1, "batch_meta_start_ts")
        elif ordered:
            add_event(ordered[0]["ts"], "START", _as_bit(ordered[0].get("start_signal")), "first_sample")

    end_ts: datetime | None = None
    end_reason = ""
    # Prioriza fim por leitura (timeline do monitor). batch_meta.end_ts fica como fallback.
    lid_open_events = [evt for evt in events if evt["type"] == "LID_OPEN"]
    if lid_open_events:
        end_ts = lid_open_events[-1]["ts"]
        end_reason = lid_open_events[-1].get("reason") or "lid_open_event"
    elif ordered:
        end_ts = ordered[-1]["ts"]
        end_reason = "last_sample_ts"
    else:
        meta_end = batch_meta.get("end_ts") if isinstance(batch_meta, dict) else None
        if isinstance(meta_end, datetime):
            end_ts = meta_end
            end_reason = "batch_meta_end_ts"

    if end_ts is not None:
        add_event(end_ts, "END", None, end_reason)

    events.sort(key=lambda evt: (evt["ts"], _event_priority(evt["type"])))
    return events


def _compute_auc(readings: list[dict], key: str) -> float | None:
    ordered = [row for row in readings if isinstance(row.get("ts"), datetime)]
    ordered.sort(key=lambda row: row["ts"])

    auc = 0.0
    segments = 0
    for left, right in zip(ordered, ordered[1:]):
        left_ts = left["ts"]
        right_ts = right["ts"]
        delta_s = (right_ts - left_ts).total_seconds()
        if delta_s <= 0:
            continue

        left_v = _to_float(left.get(key))
        right_v = _to_float(right.get(key))
        if left_v is None or right_v is None:
            continue

        auc += ((left_v + right_v) / 2.0) * delta_s
        segments += 1

    if segments <= 0:
        return None
    return auc


def compute_batch_metrics(
    readings: list[dict],
    events: list[dict] | None = None,
    batch_meta: dict | None = None,
) -> dict:
    ordered = [row for row in readings if isinstance(row.get("ts"), datetime)]
    ordered.sort(key=lambda row: row["ts"])

    temp_values = [_to_float(row.get("temp")) for row in ordered]
    temp_values = [value for value in temp_values if value is not None]

    corr_values = [_to_float(row.get("corrente")) for row in ordered]
    corr_values = [value for value in corr_values if value is not None]

    start_ts: datetime | None = None
    end_ts: datetime | None = None

    if events:
        starts = [evt["ts"] for evt in events if evt.get("type") == "START" and isinstance(evt.get("ts"), datetime)]
        ends = [evt["ts"] for evt in events if evt.get("type") == "END" and isinstance(evt.get("ts"), datetime)]
        if starts:
            start_ts = min(starts)
        if ends:
            end_ts = max(ends)

    if start_ts is None and isinstance(batch_meta, dict) and isinstance(batch_meta.get("start_ts"), datetime):
        start_ts = batch_meta["start_ts"]
    if end_ts is None and isinstance(batch_meta, dict) and isinstance(batch_meta.get("end_ts"), datetime):
        end_ts = batch_meta["end_ts"]

    if start_ts is None and ordered:
        start_ts = ordered[0]["ts"]
    if end_ts is None and ordered:
        end_ts = ordered[-1]["ts"]

    duration_s = None
    if isinstance(start_ts, datetime) and isinstance(end_ts, datetime):
        raw_duration = (end_ts - start_ts).total_seconds()
        if raw_duration >= 0:
            duration_s = raw_duration

    meta_duration_s = None
    if isinstance(batch_meta, dict):
        meta_duration_s = _to_float(batch_meta.get("duration_s"))

    # Se o duration vindo de eventos divergir muito do duration persistido do batch,
    # usa o persistido para evitar distorcoes de clock (ex.: UTC vs horario local).
    if meta_duration_s is not None and meta_duration_s >= 0:
        if duration_s is None:
            duration_s = meta_duration_s
        elif abs(duration_s - meta_duration_s) > DEFAULT_DURATION_RECONCILE_TOLERANCE_S:
            duration_s = meta_duration_s
            if ordered:
                reading_start_ts = ordered[0]["ts"]
                reading_end_ts = ordered[-1]["ts"]
                reading_duration_s = (reading_end_ts - reading_start_ts).total_seconds()
                if (
                    reading_duration_s >= 0
                    and abs(reading_duration_s - meta_duration_s)
                    <= DEFAULT_DURATION_RECONCILE_TOLERANCE_S
                ):
                    start_ts = reading_start_ts
                    end_ts = reading_end_ts
                elif isinstance(batch_meta, dict):
                    meta_start_ts = batch_meta.get("start_ts")
                    meta_end_ts = batch_meta.get("end_ts")
                    if isinstance(meta_start_ts, datetime):
                        start_ts = meta_start_ts
                    if isinstance(meta_end_ts, datetime):
                        end_ts = meta_end_ts

    current_auc = _compute_auc(ordered, "corrente")
    temp_auc = _compute_auc(ordered, "temp")
    energy_metrics = _build_energy_metrics(ordered, duration_s=duration_s)

    return {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "duration_s": _round_or_none(duration_s, 3),
        "min_temp": _round_or_none(min(temp_values) if temp_values else None),
        "max_temp": _round_or_none(max(temp_values) if temp_values else None),
        "avg_temp": _round_or_none(_avg(temp_values)),
        "min_corrente": _round_or_none(min(corr_values) if corr_values else None),
        "max_corrente": _round_or_none(max(corr_values) if corr_values else None),
        "avg_corrente": _round_or_none(_avg(corr_values)),
        "current_auc": _round_or_none(current_auc, 6),
        "temp_auc": _round_or_none(temp_auc, 6),
        "energy_proxy": _round_or_none(current_auc, 6),
        "samples": len(ordered),
        **energy_metrics,
    }


def _normalize_sources(
    readings: list[dict],
    key: str,
    start_ts: datetime,
    duration_s: float,
) -> tuple[list[float], list[float]]:
    x_values: list[float] = []
    y_values: list[float] = []

    for row in readings:
        ts = row.get("ts")
        if not isinstance(ts, datetime):
            continue

        value = _to_float(row.get(key))
        if value is None:
            continue

        if duration_s <= 0:
            pct = 0.0
        else:
            pct = ((ts - start_ts).total_seconds() / duration_s) * 100.0

        pct = max(0.0, min(100.0, pct))

        if x_values and abs(pct - x_values[-1]) < 1e-12:
            y_values[-1] = value
        else:
            x_values.append(pct)
            y_values.append(value)

    return x_values, y_values


def _interpolate_linear(x_values: list[float], y_values: list[float], x_target: list[float]) -> list[float | None]:
    if not x_values:
        return [None for _ in x_target]

    if len(x_values) == 1:
        return [y_values[0] for _ in x_target]

    out: list[float | None] = []
    for x_new in x_target:
        if x_new <= x_values[0]:
            out.append(y_values[0])
            continue
        if x_new >= x_values[-1]:
            out.append(y_values[-1])
            continue

        left_index = bisect_right(x_values, x_new) - 1
        right_index = left_index + 1

        x0 = x_values[left_index]
        x1 = x_values[right_index]
        y0 = y_values[left_index]
        y1 = y_values[right_index]

        if abs(x1 - x0) < 1e-12:
            out.append(y1)
            continue

        factor = (x_new - x0) / (x1 - x0)
        out.append(y0 + (y1 - y0) * factor)

    return out


def infer_stage_markers(
    temp_profile: list[float | None],
    corrente_profile: list[float | None],
    lid_profile: list[float | None] | None = None,
) -> dict:
    markers = {
        "ramp_end_pct": None,
        "plateau_start_pct": None,
        "discharge_pct": None,
    }

    temp_values = [value for value in temp_profile if value is not None]
    if temp_values:
        temp_threshold = 0.9 * max(temp_values)
        for idx, value in enumerate(temp_profile):
            if value is not None and value >= temp_threshold:
                markers["ramp_end_pct"] = idx
                markers["plateau_start_pct"] = idx
                break

    corr_values = [value for value in corrente_profile if value is not None]
    has_lid_data = bool(lid_profile) and any(value is not None for value in lid_profile or [])
    if corr_values:
        corr_threshold = 0.3 * max(corr_values)
        for idx, value in enumerate(corrente_profile):
            if value is None or value > corr_threshold:
                continue

            if has_lid_data:
                if idx >= len(lid_profile or []):
                    continue
                lid_value = lid_profile[idx]
                if lid_value is None or lid_value > 0.5:
                    continue

            markers["discharge_pct"] = idx
            break

        if markers["discharge_pct"] is None and has_lid_data:
            for idx, value in enumerate(corrente_profile):
                if value is not None and value <= corr_threshold:
                    markers["discharge_pct"] = idx
                    break

    return markers


def resample_profile_101(readings: list[dict], points: int = 101) -> dict:
    if points < 2:
        raise ValueError("points must be >= 2")

    ordered = [row for row in readings if isinstance(row.get("ts"), datetime)]
    ordered.sort(key=lambda row: row["ts"])

    time_pct = [round(idx * 100.0 / float(points - 1), 6) for idx in range(points)]

    if not ordered:
        blank = [None for _ in range(points)]
        return {
            "time_pct": time_pct,
            "temp": blank,
            "corrente": blank,
            "markers": infer_stage_markers(blank, blank, None),
        }

    start_ts = ordered[0]["ts"]
    end_ts = ordered[-1]["ts"]
    duration_s = max(0.0, (end_ts - start_ts).total_seconds())

    temp_x, temp_y = _normalize_sources(ordered, "temp", start_ts, duration_s)
    corr_x, corr_y = _normalize_sources(ordered, "corrente", start_ts, duration_s)
    lid_x, lid_y = _normalize_sources(ordered, "lid_signal", start_ts, duration_s)

    temp_profile = _interpolate_linear(temp_x, temp_y, time_pct)
    corrente_profile = _interpolate_linear(corr_x, corr_y, time_pct)
    lid_profile = _interpolate_linear(lid_x, lid_y, time_pct)

    temp_profile = [_round_or_none(value, 4) for value in temp_profile]
    corrente_profile = [_round_or_none(value, 4) for value in corrente_profile]
    lid_profile = [_round_or_none(value, 4) for value in lid_profile]

    markers = infer_stage_markers(temp_profile, corrente_profile, lid_profile)

    payload = {
        "time_pct": time_pct,
        "temp": temp_profile,
        "corrente": corrente_profile,
        "markers": markers,
    }
    if any(value is not None for value in lid_profile):
        payload["lid"] = lid_profile
    return payload


def _quantize(value: float | None, decimals: int = 1) -> str:
    if value is None:
        return "na"
    return f"{round(float(value), decimals):.{decimals}f}"


def build_signature_from_profile(
    temp_profile: list[float | None],
    corrente_profile: list[float | None],
    decimals: int = 1,
) -> str:
    left = ",".join(_quantize(value, decimals=decimals) for value in temp_profile)
    right = ",".join(_quantize(value, decimals=decimals) for value in corrente_profile)
    payload = f"temp:{left}|corr:{right}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_signature_payload(readings: list[dict]) -> dict:
    profile = resample_profile_101(readings)
    signature = build_signature_from_profile(profile["temp"], profile["corrente"])
    return {
        "signature": signature,
        "profile": profile,
    }


def compute_rmse(reference: list[float | None], target: list[float | None]) -> float | None:
    sum_sq = 0.0
    count = 0

    for left, right in zip(reference, target):
        left_value = _to_float(left)
        right_value = _to_float(right)
        if left_value is None or right_value is None:
            continue
        diff = right_value - left_value
        sum_sq += diff * diff
        count += 1

    if count <= 0:
        return None
    return math.sqrt(sum_sq / float(count))


def build_delta_profile(
    baseline_profile: dict,
    target_profile: dict,
) -> dict:
    baseline_temp = baseline_profile.get("temp") or []
    baseline_corr = baseline_profile.get("corrente") or []
    target_temp = target_profile.get("temp") or []
    target_corr = target_profile.get("corrente") or []

    length = min(len(baseline_temp), len(target_temp), len(baseline_corr), len(target_corr))

    temp_delta: list[float | None] = []
    corr_delta: list[float | None] = []

    for idx in range(length):
        base_temp_value = _to_float(baseline_temp[idx])
        target_temp_value = _to_float(target_temp[idx])
        if base_temp_value is None or target_temp_value is None:
            temp_delta.append(None)
        else:
            temp_delta.append(_round_or_none(target_temp_value - base_temp_value, 4))

        base_corr_value = _to_float(baseline_corr[idx])
        target_corr_value = _to_float(target_corr[idx])
        if base_corr_value is None or target_corr_value is None:
            corr_delta.append(None)
        else:
            corr_delta.append(_round_or_none(target_corr_value - base_corr_value, 4))

    return {
        "time_pct": (baseline_profile.get("time_pct") or [])[:length],
        "temp_delta": temp_delta,
        "corr_delta": corr_delta,
    }


def _diff_or_none(left: Any, right: Any, places: int = 4) -> float | None:
    left_value = _to_float(left)
    right_value = _to_float(right)
    if left_value is None or right_value is None:
        return None
    return round(right_value - left_value, places)


def _diff_pct_or_none(left: Any, right: Any, places: int = 4) -> float | None:
    left_value = _to_float(left)
    right_value = _to_float(right)
    if left_value is None or right_value is None:
        return None
    if abs(left_value) < 1e-12:
        return None
    return round(((right_value - left_value) / left_value) * 100.0, places)


def compute_compare_metrics(
    baseline_profile: dict,
    target_profile: dict,
    baseline_metrics: dict,
    target_metrics: dict,
) -> dict:
    energy_proxy_diff = _diff_or_none(
        baseline_metrics.get("energy_proxy"),
        target_metrics.get("energy_proxy"),
        places=6,
    )
    baseline_energy_est_kwh = baseline_metrics.get("energy_est_kwh")
    if baseline_energy_est_kwh is None:
        baseline_energy_est_kwh = baseline_metrics.get("energy_estimated_kwh")

    target_energy_est_kwh = target_metrics.get("energy_est_kwh")
    if target_energy_est_kwh is None:
        target_energy_est_kwh = target_metrics.get("energy_estimated_kwh")

    energy_est_kwh_diff = _diff_or_none(
        baseline_energy_est_kwh,
        target_energy_est_kwh,
        places=9,
    )

    return {
        "rmse_temp": _round_or_none(
            compute_rmse(baseline_profile.get("temp") or [], target_profile.get("temp") or []),
            6,
        ),
        "rmse_corrente": _round_or_none(
            compute_rmse(baseline_profile.get("corrente") or [], target_profile.get("corrente") or []),
            6,
        ),
        "duration_diff_s": _diff_or_none(
            baseline_metrics.get("duration_s"),
            target_metrics.get("duration_s"),
            places=3,
        ),
        "current_auc_diff": _diff_or_none(
            baseline_metrics.get("current_auc"),
            target_metrics.get("current_auc"),
            places=6,
        ),
        "energy_proxy_diff": energy_proxy_diff,
        "energy_est_kwh_diff": energy_est_kwh_diff,
        "energy_est_kwh_diff_pct": _diff_pct_or_none(
            baseline_energy_est_kwh,
            target_energy_est_kwh,
            places=4,
        ),
        # Chave antiga mantida por compatibilidade.
        "energy_estimated_kwh_diff": energy_est_kwh_diff,
        "peak_temp_diff": _diff_or_none(
            baseline_metrics.get("max_temp"),
            target_metrics.get("max_temp"),
            places=4,
        ),
        "peak_corrente_diff": _diff_or_none(
            baseline_metrics.get("max_corrente"),
            target_metrics.get("max_corrente"),
            places=4,
        ),
    }

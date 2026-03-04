'''Tests for aisdb.anomaly.spoof_detect — AIS Spoofing Detection.

Covers:
  - Helper functions (_haversine_m, _bearing_deg, _angular_diff_deg, _sigmoid)
  - Per-ping indicator scorers (_kinematic_score, _sog_delta_score, _cog_delta_score, _interval_score)
  - SpoofEvent dataclass
  - SpoofingDetector.score_track(), .annotate(), .flag_spoofing()
  - SpoofingDetector.check_mmsi_conflicts()
  - Save / load round-trip
  - flag_spoofing() module-level convenience function
'''

import math
import pickle
import tempfile
from pathlib import Path

import numpy as np
import pytest

from aisdb.anomaly.spoof_detect import (
    COG_TOLERANCE_DEG,
    MIN_CREDIBLE_INTERVAL_S,
    MMSI_CONFLICT_SPEED_KN,
    NOMINAL_INTERVAL_S,
    SOG_TOLERANCE_KN,
    SpoofEvent,
    SpoofingDetector,
    _angular_diff_deg,
    _bearing_deg,
    _cog_delta_score,
    _haversine_m,
    _interval_score,
    _kinematic_score,
    _sigmoid,
    _sog_delta_score,
    flag_spoofing,
)

# ---------------------------------------------------------------------------
# Track builders
# ---------------------------------------------------------------------------

def _make_track(
    mmsi: int = 123456789,
    n: int = 20,
    lat_start: float = 55.0,
    lon_start: float = 10.0,
    dt_s: float = 10.0,
    speed_kn: float = 8.0,
    ship_type: str = 'cargo',
    sog_override: float = None,
    cog_override: float = None,
    t0: int = 1_700_000_000,
) -> dict:
    '''Straight-line track heading ~North.'''
    # 1 knot = 0.514444 m/s; 1 deg lat ≈ 111,000 m
    lat_step = speed_kn * 0.514444 / 111_000 * dt_s
    lats = np.array([lat_start + i * lat_step for i in range(n)], dtype=np.float32)
    lons = np.full(n, lon_start, dtype=np.float32)
    times = np.array([t0 + i * int(dt_s) for i in range(n)], dtype=np.uint32)
    cog_val = 0.0 if cog_override is None else cog_override
    sog_val = speed_kn if sog_override is None else sog_override
    return {
        'mmsi': mmsi,
        'ship_type_txt': ship_type,
        'time': times,
        'lat': lats,
        'lon': lons,
        'sog': np.full(n, sog_val, dtype=np.float32),
        'cog': np.full(n, cog_val, dtype=np.float32),
        'dynamic': set(),
        'static': {'mmsi', 'ship_type_txt'},
    }


def _make_teleporting_track(mmsi: int = 111111111) -> dict:
    '''A vessel that "teleports" 1000 km between two consecutive pings.'''
    n = 10
    t0 = 1_700_000_000
    dt = 10  # 10 seconds between pings
    # First 5 pings near Norway, last 5 near Spain (≈ 3000 km apart)
    lats = np.array([60.0] * 5 + [40.0] * 5, dtype=np.float32)
    lons = np.array([5.0] * 5 + [5.0] * 5, dtype=np.float32)
    times = np.array([t0 + i * dt for i in range(n)], dtype=np.uint32)
    return {
        'mmsi': mmsi,
        'ship_type_txt': 'cargo',
        'time': times,
        'lat': lats,
        'lon': lons,
        'sog': np.full(n, 8.0, dtype=np.float32),    # reports 8 kn
        'cog': np.full(n, 0.0, dtype=np.float32),
        'dynamic': set(),
        'static': set(),
    }


def _make_sog_mismatch_track(mmsi: int = 222222222) -> dict:
    '''Vessel moves at ~30 kn but reports 5 kn SOG.'''
    n = 10
    t0 = 1_700_000_000
    dt = 10  # seconds
    speed_kn = 30.0  # actual movement speed
    lat_step = speed_kn * 0.514444 / 111_000 * dt
    lats = np.array([55.0 + i * lat_step for i in range(n)], dtype=np.float32)
    lons = np.full(n, 10.0, dtype=np.float32)
    times = np.array([t0 + i * dt for i in range(n)], dtype=np.uint32)
    return {
        'mmsi': mmsi,
        'ship_type_txt': 'default',
        'time': times,
        'lat': lats,
        'lon': lons,
        'sog': np.full(n, 5.0, dtype=np.float32),   # reports 5 kn, moves at 30
        'cog': np.full(n, 0.0, dtype=np.float32),
        'dynamic': set(),
        'static': set(),
    }


def _make_cog_mismatch_track(mmsi: int = 333333333) -> dict:
    '''Vessel moves North but reports COG=180° (South).'''
    n = 10
    t0 = 1_700_000_000
    dt = 10
    speed_kn = 10.0
    lat_step = speed_kn * 0.514444 / 111_000 * dt
    lats = np.array([55.0 + i * lat_step for i in range(n)], dtype=np.float32)
    lons = np.full(n, 10.0, dtype=np.float32)
    times = np.array([t0 + i * dt for i in range(n)], dtype=np.uint32)
    return {
        'mmsi': mmsi,
        'ship_type_txt': 'default',
        'time': times,
        'lat': lats,
        'lon': lons,
        'sog': np.full(n, speed_kn, dtype=np.float32),
        'cog': np.full(n, 180.0, dtype=np.float32),  # reports South
        'dynamic': set(),
        'static': set(),
    }


def _make_rapid_interval_track(mmsi: int = 444444444) -> dict:
    '''Track with sub-second transmission intervals — clearly injected.'''
    n = 10
    t0 = 1_700_000_000
    lats = np.linspace(55.0, 55.01, n, dtype=np.float32)
    lons = np.full(n, 10.0, dtype=np.float32)
    # Sub-second intervals (0.5 s)
    times = np.array([t0 + i for i in range(n)], dtype=np.uint32)
    # Override: use actual sub-second via float array cast
    times = (np.full(n, t0) + np.arange(n) * 1).astype(np.uint32)  # 1s intervals still
    return {
        'mmsi': mmsi,
        'ship_type_txt': 'tanker',
        'time': times,
        'lat': lats,
        'lon': lons,
        'sog': np.full(n, 5.0, dtype=np.float32),
        'cog': np.full(n, 0.0, dtype=np.float32),
        'dynamic': set(),
        'static': set(),
    }


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

class TestHaversine:
    def test_zero_distance(self):
        assert _haversine_m(55.0, 10.0, 55.0, 10.0) == pytest.approx(0.0)

    def test_known_distance(self):
        # London (51.5, -0.1) to Paris (48.85, 2.35) ≈ 341 km
        dist = _haversine_m(51.5, -0.1, 48.85, 2.35)
        assert 330_000 < dist < 360_000

    def test_symmetry(self):
        d1 = _haversine_m(55.0, 10.0, 58.0, 14.0)
        d2 = _haversine_m(58.0, 14.0, 55.0, 10.0)
        assert d1 == pytest.approx(d2, rel=1e-6)

    def test_equatorial_degree(self):
        # 1 degree of longitude at equator ≈ 111,319 m
        dist = _haversine_m(0.0, 0.0, 0.0, 1.0)
        assert 111_000 < dist < 112_000


class TestBearing:
    def test_north(self):
        # Moving north (increasing lat, same lon)
        b = _bearing_deg(55.0, 10.0, 56.0, 10.0)
        assert b == pytest.approx(0.0, abs=1.0)

    def test_east(self):
        b = _bearing_deg(0.0, 0.0, 0.0, 1.0)
        assert b == pytest.approx(90.0, abs=1.0)

    def test_south(self):
        b = _bearing_deg(56.0, 10.0, 55.0, 10.0)
        assert b == pytest.approx(180.0, abs=1.0)

    def test_west(self):
        b = _bearing_deg(0.0, 1.0, 0.0, 0.0)
        assert b == pytest.approx(270.0, abs=1.0)

    def test_range(self):
        for lat1, lon1, lat2, lon2 in [
            (0, 0, 1, 1), (55, 10, 56, 11), (-33, 151, -34, 150)
        ]:
            b = _bearing_deg(lat1, lon1, lat2, lon2)
            assert 0.0 <= b < 360.0


class TestAngularDiff:
    def test_zero(self):
        assert _angular_diff_deg(90.0, 90.0) == pytest.approx(0.0)

    def test_opposite(self):
        assert _angular_diff_deg(0.0, 180.0) == pytest.approx(180.0)

    def test_wrap_around(self):
        # 350° vs 10° → diff = 20°
        assert _angular_diff_deg(350.0, 10.0) == pytest.approx(20.0)

    def test_symmetry(self):
        assert _angular_diff_deg(30.0, 90.0) == _angular_diff_deg(90.0, 30.0)

    def test_max_is_180(self):
        for a, b in [(0, 180), (1, 181), (90, 270)]:
            assert _angular_diff_deg(a, b) <= 180.0


class TestSigmoid:
    def test_at_centre(self):
        # At centre, sigmoid should be 0.5
        s = _sigmoid(10.0, 10.0, 2.0)
        assert s == pytest.approx(0.5, abs=0.01)

    def test_far_above_centre(self):
        s = _sigmoid(100.0, 10.0, 2.0)
        assert s > 0.99

    def test_far_below_centre(self):
        s = _sigmoid(-100.0, 10.0, 2.0)
        assert s < 0.01

    def test_range(self):
        for x in [-10, 0, 5, 10, 20]:
            s = _sigmoid(x, 5.0, 2.0)
            assert 0.0 <= s <= 1.0


# ---------------------------------------------------------------------------
# Indicator scorer tests
# ---------------------------------------------------------------------------

class TestKinematicScore:
    def test_well_within_envelope(self):
        # Fishing vessel max 15 kn — 5 kn should score very low
        s = _kinematic_score(5.0, 15.0)
        assert s < 0.2

    def test_exactly_at_limit(self):
        s = _kinematic_score(15.0, 15.0)
        assert s == pytest.approx(0.5, abs=0.05)

    def test_far_above_limit(self):
        # 200 kn for a cargo vessel (max 25 kn) — should score very high
        s = _kinematic_score(200.0, 25.0)
        assert s > 0.99

    def test_scores_in_range(self):
        for kn in [0.0, 5.0, 15.0, 30.0, 100.0]:
            s = _kinematic_score(kn, 20.0)
            assert 0.0 <= s <= 1.0


class TestSOGDeltaScore:
    def test_exact_match(self):
        s = _sog_delta_score(10.0, 10.0)
        assert s < 0.2

    def test_small_mismatch(self):
        s = _sog_delta_score(10.0, 11.0)
        assert s < 0.3

    def test_large_mismatch(self):
        # 30 kn vs 5 kn reported → delta = 25 kn >> SOG_TOLERANCE_KN
        s = _sog_delta_score(30.0, 5.0)
        assert s > 0.8

    def test_ais_unavailable_sentinel(self):
        # SOG > 102.2 → not available, should score 0
        s = _sog_delta_score(10.0, 102.3)
        assert s == 0.0

    def test_scores_in_range(self):
        for delta in [0.0, 2.0, 5.0, 15.0, 30.0]:
            s = _sog_delta_score(10.0 + delta, 10.0)
            assert 0.0 <= s <= 1.0


class TestCOGDeltaScore:
    def test_consistent_cog(self):
        # Moving North (bearing ≈ 0°), reporting COG 0° → no mismatch
        s = _cog_delta_score(0.0, 0.0)
        assert s < 0.2

    def test_opposite_cog(self):
        # Moving North but reporting COG 180° → severe mismatch
        s = _cog_delta_score(0.0, 180.0)
        assert s > 0.9

    def test_ais_unavailable_sentinel(self):
        # COG ≥ 360° → not available
        s = _cog_delta_score(0.0, 360.0)
        assert s == 0.0

    def test_within_tolerance(self):
        # 15° difference → within 30° tolerance
        s = _cog_delta_score(0.0, 15.0)
        assert s < 0.5

    def test_scores_in_range(self):
        for diff in [0.0, 15.0, 30.0, 90.0, 180.0]:
            s = _cog_delta_score(0.0, diff)
            assert 0.0 <= s <= 1.0


class TestIntervalScore:
    def test_normal_interval(self):
        # 10 s interval for cargo (nominal 10 s) → no flag
        s = _interval_score(10.0, NOMINAL_INTERVAL_S['cargo'])
        assert s == 0.0

    def test_sub_second_interval(self):
        # 0.5 s → always score 1.0 (injected)
        # Note: time array is uint32 so minimum interval is 1s in our track builders;
        # test the scorer directly with float
        s = _interval_score(0.5, 10.0)
        assert s == 1.0

    def test_below_minimum(self):
        s = _interval_score(MIN_CREDIBLE_INTERVAL_S - 0.1, 10.0)
        assert s == 1.0

    def test_extremely_rapid_vs_nominal(self):
        # 0.1 s interval vs 180 s nominal → ratio = 1800 → should flag
        s = _interval_score(0.1, 180.0)
        assert s == 1.0  # below MIN_CREDIBLE_INTERVAL_S

    def test_scores_in_range(self):
        for dt in [1.0, 2.0, 5.0, 10.0, 60.0, 300.0]:
            s = _interval_score(dt, 10.0)
            assert 0.0 <= s <= 1.0


# ---------------------------------------------------------------------------
# SpoofEvent tests
# ---------------------------------------------------------------------------

class TestSpoofEvent:
    def test_creation(self):
        ev = SpoofEvent(
            mmsi=123456789,
            epoch=1_700_000_000,
            lat=55.0,
            lon=10.0,
            spoof_score=0.85,
            spoof_type='kinematic',
        )
        assert ev.mmsi == 123456789
        assert ev.spoof_score == pytest.approx(0.85)
        assert ev.spoof_type == 'kinematic'

    def test_dt_property(self):
        ev = SpoofEvent(
            mmsi=1,
            epoch=1_700_000_000,
            lat=0.0,
            lon=0.0,
            spoof_score=0.5,
        )
        dt = ev.dt
        assert dt.year == 2023
        assert dt.tzinfo is None   # naive UTC

    def test_repr(self):
        ev = SpoofEvent(
            mmsi=987654321,
            epoch=1_700_000_000,
            lat=55.0,
            lon=10.0,
            spoof_score=0.75,
            spoof_type='sog_delta',
            computed_speed_kn=30.0,
            reported_speed_kn=5.0,
        )
        r = repr(ev)
        assert '987654321' in r
        assert 'sog_delta' in r
        assert '0.750' in r

    def test_defaults(self):
        ev = SpoofEvent(mmsi=1, epoch=0, lat=0.0, lon=0.0, spoof_score=0.0)
        assert ev.spoof_type == 'unknown'
        assert ev.computed_speed_kn == 0.0
        assert ev.details == ''


# ---------------------------------------------------------------------------
# SpoofingDetector — score_track
# ---------------------------------------------------------------------------

class TestScoreTrack:
    def setup_method(self):
        self.det = SpoofingDetector()

    def test_single_ping_returns_empty(self):
        track = _make_track(n=1)
        composite, kin, sog_d, cog_d, intv = self.det.score_track(track)
        for arr in (composite, kin, sog_d, cog_d, intv):
            assert len(arr) == 0

    def test_output_shape(self):
        track = _make_track(n=10)
        composite, kin, sog_d, cog_d, intv = self.det.score_track(track)
        for arr in (composite, kin, sog_d, cog_d, intv):
            assert len(arr) == 9   # n - 1

    def test_normal_track_low_scores(self):
        track = _make_track(n=20, speed_kn=8.0, ship_type='cargo')
        composite, kin, sog_d, cog_d, intv = self.det.score_track(track)
        assert np.max(composite) < 0.5

    def test_teleporting_track_high_kinematic(self):
        track = _make_teleporting_track()
        composite, kin, sog_d, cog_d, intv = self.det.score_track(track)
        # The jump from ping 4 to ping 5 crosses 20 degrees lat in 10 s
        assert np.max(kin) > 0.9

    def test_sog_mismatch_high_sog_score(self):
        track = _make_sog_mismatch_track()
        _, _, sog_d, _, _ = self.det.score_track(track)
        assert np.max(sog_d) > 0.8

    def test_cog_mismatch_high_cog_score(self):
        track = _make_cog_mismatch_track()
        _, _, _, cog_d, _ = self.det.score_track(track)
        assert np.max(cog_d) > 0.9

    def test_scores_bounded(self):
        track = _make_teleporting_track()
        composite, kin, sog_d, cog_d, intv = self.det.score_track(track)
        for arr in (composite, kin, sog_d, cog_d, intv):
            assert np.all(arr >= 0.0)
            assert np.all(arr <= 1.0)

    def test_tanker_interval_flag(self):
        # Tanker nominal interval = 10 s. Sub-2s intervals should flag.
        track = _make_track(n=10, ship_type='tanker', dt_s=1.0)
        _, _, _, _, intv = self.det.score_track(track)
        # 1s interval is below MIN_CREDIBLE_INTERVAL_S=2, so all should be 1.0
        assert np.all(intv == 1.0)


# ---------------------------------------------------------------------------
# SpoofingDetector — annotate
# ---------------------------------------------------------------------------

class TestAnnotate:
    def setup_method(self):
        self.det = SpoofingDetector()

    def test_adds_keys(self):
        track = _make_track(n=10)
        result = self.det.annotate(track)
        for key in ('spoof_scores', 'spoof_kinematic', 'spoof_sog', 'spoof_cog',
                    'spoof_interval', 'spoof_events'):
            assert key in result

    def test_dynamic_updated(self):
        track = _make_track(n=10)
        result = self.det.annotate(track)
        assert 'spoof_scores' in result['dynamic']
        assert 'spoof_kinematic' in result['dynamic']

    def test_single_ping_no_events(self):
        track = _make_track(n=1)
        result = self.det.annotate(track)
        assert result['spoof_events'] == []

    def test_normal_track_no_events(self):
        track = _make_track(n=20, speed_kn=8.0, ship_type='cargo')
        result = self.det.annotate(track, threshold=0.5)
        assert len(result['spoof_events']) == 0

    def test_teleport_produces_events(self):
        track = _make_teleporting_track()
        result = self.det.annotate(track, threshold=0.3)
        assert len(result['spoof_events']) > 0

    def test_event_type_kinematic_for_teleport(self):
        track = _make_teleporting_track()
        result = self.det.annotate(track, threshold=0.3)
        types = {e.spoof_type for e in result['spoof_events']}
        assert 'kinematic' in types

    def test_high_threshold_suppresses_events(self):
        track = _make_sog_mismatch_track()
        result = self.det.annotate(track, threshold=0.99)
        assert len(result['spoof_events']) == 0

    def test_low_threshold_catches_more(self):
        track = _make_sog_mismatch_track()
        r_low = self.det.annotate(dict(track), threshold=0.1)
        r_high = self.det.annotate(dict(track), threshold=0.9)
        assert len(r_low['spoof_events']) >= len(r_high['spoof_events'])

    def test_event_fields_populated(self):
        track = _make_teleporting_track()
        result = self.det.annotate(track, threshold=0.3)
        ev = result['spoof_events'][0]
        assert ev.mmsi == track['mmsi']
        assert 0.0 <= ev.spoof_score <= 1.0
        assert ev.computed_speed_kn > 0.0
        assert ev.details != ''


# ---------------------------------------------------------------------------
# SpoofingDetector — flag_spoofing generator
# ---------------------------------------------------------------------------

class TestFlagSpoofing:
    def test_yields_all_tracks(self):
        tracks = [_make_track(mmsi=i) for i in range(5)]
        det = SpoofingDetector()
        results = list(det.flag_spoofing(iter(tracks)))
        assert len(results) == 5

    def test_tracks_annotated(self):
        tracks = [_make_track()]
        det = SpoofingDetector()
        for track in det.flag_spoofing(iter(tracks)):
            assert 'spoof_events' in track

    def test_spoofed_track_detected(self):
        tracks = [_make_teleporting_track()]
        det = SpoofingDetector()
        results = list(det.flag_spoofing(iter(tracks), threshold=0.3))
        assert len(results[0]['spoof_events']) > 0


# ---------------------------------------------------------------------------
# SpoofingDetector — MMSI conflict detection
# ---------------------------------------------------------------------------

class TestMMSIConflict:
    def _conflict_tracks(self, mmsi: int = 999999999) -> list:
        '''Two tracks with the same MMSI at opposite ends of the world.'''
        t0 = 1_700_000_000
        # Track A: Norwegian waters
        a = {
            'mmsi': mmsi,
            'ship_type_txt': 'cargo',
            'time': np.array([t0, t0 + 10], dtype=np.uint32),
            'lat': np.array([60.0, 60.001], dtype=np.float32),
            'lon': np.array([5.0, 5.001], dtype=np.float32),
            'sog': np.full(2, 5.0, dtype=np.float32),
            'cog': np.full(2, 90.0, dtype=np.float32),
            'dynamic': set(), 'static': set(),
        }
        # Track B: same MMSI, 10 seconds later, far away (Spain)
        b = {
            'mmsi': mmsi,
            'ship_type_txt': 'cargo',
            'time': np.array([t0 + 20, t0 + 30], dtype=np.uint32),
            'lat': np.array([40.0, 40.001], dtype=np.float32),
            'lon': np.array([5.0, 5.001], dtype=np.float32),
            'sog': np.full(2, 5.0, dtype=np.float32),
            'cog': np.full(2, 90.0, dtype=np.float32),
            'dynamic': set(), 'static': set(),
        }
        return [a, b]

    def test_detects_conflict(self):
        det = SpoofingDetector()
        tracks = self._conflict_tracks()
        results = list(det.check_mmsi_conflicts(iter(tracks)))
        # Second track should have a conflict event
        conflict_events = [e for e in results[1]['spoof_events']
                           if e.spoof_type == 'mmsi_conflict']
        assert len(conflict_events) == 1

    def test_conflict_event_score_above_threshold(self):
        det = SpoofingDetector()
        tracks = self._conflict_tracks()
        results = list(det.check_mmsi_conflicts(iter(tracks)))
        conflict = next(e for e in results[1]['spoof_events']
                        if e.spoof_type == 'mmsi_conflict')
        assert conflict.spoof_score > 0.5

    def test_no_conflict_same_location(self):
        det = SpoofingDetector()
        mmsi = 777777777
        t0 = 1_700_000_000
        t_a = {
            'mmsi': mmsi, 'ship_type_txt': 'cargo',
            'time': np.array([t0, t0 + 10], dtype=np.uint32),
            'lat': np.array([55.0, 55.001], dtype=np.float32),
            'lon': np.array([10.0, 10.001], dtype=np.float32),
            'sog': np.full(2, 8.0, dtype=np.float32),
            'cog': np.full(2, 90.0, dtype=np.float32),
            'dynamic': set(), 'static': set(),
        }
        t_b = {
            'mmsi': mmsi, 'ship_type_txt': 'cargo',
            'time': np.array([t0 + 3600, t0 + 3610], dtype=np.uint32),
            'lat': np.array([55.05, 55.051], dtype=np.float32),   # ~5 km away
            'lon': np.array([10.0, 10.001], dtype=np.float32),
            'sog': np.full(2, 8.0, dtype=np.float32),
            'cog': np.full(2, 0.0, dtype=np.float32),
            'dynamic': set(), 'static': set(),
        }
        results = list(det.check_mmsi_conflicts(iter([t_a, t_b])))
        conflicts = [e for e in results[1]['spoof_events']
                     if e.spoof_type == 'mmsi_conflict']
        assert len(conflicts) == 0

    def test_registry_updated(self):
        det = SpoofingDetector()
        track = _make_track(mmsi=555555555, n=5)
        list(det.check_mmsi_conflicts(iter([track])))
        assert 555555555 in det._mmsi_registry

    def test_reset_clears_registry(self):
        det = SpoofingDetector()
        track = _make_track(mmsi=555555555, n=5)
        list(det.check_mmsi_conflicts(iter([track])))
        det.reset_mmsi_registry()
        assert len(det._mmsi_registry) == 0

    def test_conflict_speed_threshold(self):
        # Two tracks only 1 km apart in 10 s → ~200 kn → conflict
        # vs same MMSI but 1 m apart → no conflict
        det = SpoofingDetector()
        tracks = self._conflict_tracks(mmsi=888888888)
        results = list(det.check_mmsi_conflicts(iter(tracks)))
        conflict_events = [e for e in results[1]['spoof_events']
                           if e.spoof_type == 'mmsi_conflict']
        assert len(conflict_events) == 1
        assert conflict_events[0].computed_speed_kn > MMSI_CONFLICT_SPEED_KN


# ---------------------------------------------------------------------------
# Module-level flag_spoofing convenience function
# ---------------------------------------------------------------------------

class TestFlagSpoofingFunction:
    def test_returns_generator(self):
        import types
        tracks = [_make_track()]
        result = flag_spoofing(iter(tracks))
        assert isinstance(result, types.GeneratorType)

    def test_creates_detector_if_none(self):
        tracks = [_make_track()]
        results = list(flag_spoofing(iter(tracks)))
        assert 'spoof_events' in results[0]

    def test_passes_threshold(self):
        tracks = [_make_teleporting_track()]
        results_low = list(flag_spoofing(iter(tracks), threshold=0.1))
        tracks2 = [_make_teleporting_track()]
        results_high = list(flag_spoofing(iter(tracks2), threshold=0.99))
        assert len(results_low[0]['spoof_events']) >= len(results_high[0]['spoof_events'])

    def test_uses_existing_detector(self):
        det = SpoofingDetector()
        tracks = [_make_track(mmsi=123)]
        list(flag_spoofing(iter(tracks), detector=det))
        # Detector's registry should be unchanged (flag_spoofing doesn't call check_mmsi_conflicts)
        assert len(det._mmsi_registry) == 0


# ---------------------------------------------------------------------------
# Save / load round-trip
# ---------------------------------------------------------------------------

class TestSaveLoad:
    def test_save_load_empty_registry(self):
        det = SpoofingDetector()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / 'spoof.pkl')
            det.save(path)
            loaded = SpoofingDetector.load(path)
        assert isinstance(loaded, SpoofingDetector)
        assert len(loaded._mmsi_registry) == 0

    def test_save_load_with_registry(self):
        det = SpoofingDetector()
        det._mmsi_registry[123456789] = (1_700_000_000, 55.0, 10.0)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / 'spoof.pkl')
            det.save(path)
            loaded = SpoofingDetector.load(path)
        assert 123456789 in loaded._mmsi_registry
        assert loaded._mmsi_registry[123456789] == (1_700_000_000, 55.0, 10.0)

    def test_loaded_detector_still_works(self):
        det = SpoofingDetector()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / 'spoof.pkl')
            det.save(path)
            loaded = SpoofingDetector.load(path)
        track = _make_teleporting_track()
        result = loaded.annotate(track, threshold=0.3)
        assert 'spoof_events' in result


# ---------------------------------------------------------------------------
# Repr tests
# ---------------------------------------------------------------------------

class TestRepr:
    def test_spoofing_detector_repr(self):
        det = SpoofingDetector()
        r = repr(det)
        assert 'SpoofingDetector' in r
        assert '0' in r  # 0 entries in registry

    def test_spoof_event_repr(self):
        ev = SpoofEvent(
            mmsi=123456789,
            epoch=1_700_000_000,
            lat=55.0,
            lon=10.0,
            spoof_score=0.85,
            spoof_type='kinematic',
            computed_speed_kn=100.0,
            reported_speed_kn=8.0,
        )
        r = repr(ev)
        assert '123456789' in r
        assert 'kinematic' in r

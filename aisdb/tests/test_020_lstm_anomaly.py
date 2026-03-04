'''Tests for aisdb.anomaly.lstm_anomaly — LSTM/statistical trajectory anomaly detection.'''

import sys
from datetime import datetime, timedelta

import numpy as np
import pytest
import torch

from aisdb.anomaly.lstm_anomaly import (
    AnomalyEvent,
    TrajectoryAnomalyDetector,
    _extract_features,
    _infer_anomaly_type,
    _sliding_windows,
    _window_scores_to_ping_scores,
    flag_anomalies,
    N_FEATURES,
    DEFAULT_WINDOW,
)


# ---------------------------------------------------------------------------
# Synthetic track builders
# ---------------------------------------------------------------------------

def _make_track(n=60, sog=8.0, mmsi=123456789, ship_type_txt='cargo',
                start_epoch=1_620_000_000, cog=90.0, jitter=0.0):
    '''Straight-line track at constant speed and course.'''
    t0 = start_epoch
    rng = np.random.default_rng(42)
    times = np.array([t0 + i * 10 for i in range(n)], dtype=np.uint32)
    lons = np.linspace(-63.5, -63.0, n, dtype=np.float32)
    lats = np.linspace(44.5, 44.8, n, dtype=np.float32)
    if jitter > 0:
        lons += rng.uniform(-jitter, jitter, n).astype(np.float32)
        lats += rng.uniform(-jitter, jitter, n).astype(np.float32)
    sog_arr = np.full(n, sog, dtype=np.float32)
    cog_arr = np.full(n, cog, dtype=np.uint32)
    return dict(
        mmsi=mmsi,
        ship_type_txt=ship_type_txt,
        time=times,
        lon=lons,
        lat=lats,
        sog=sog_arr,
        cog=cog_arr,
        static={'mmsi', 'ship_type_txt'},
        dynamic={'time', 'lon', 'lat', 'sog', 'cog'},
    )


def _make_loitering_track(n=80, mmsi=999):
    '''Vessel moving very slowly in a tiny area — loitering pattern.'''
    rng = np.random.default_rng(7)
    t0 = 1_620_000_000
    times = np.array([t0 + i * 30 for i in range(n)], dtype=np.uint32)
    # Random walk in a 0.01 degree box
    lons = np.cumsum(rng.uniform(-0.001, 0.001, n)).astype(np.float32) - 63.5
    lats = np.cumsum(rng.uniform(-0.001, 0.001, n)).astype(np.float32) + 44.5
    sog = np.full(n, 0.3, dtype=np.float32)
    cog = (rng.uniform(0, 360, n)).astype(np.uint32)
    return dict(
        mmsi=mmsi, ship_type_txt='fishing',
        time=times, lon=lons, lat=lats, sog=sog, cog=cog,
        static={'mmsi', 'ship_type_txt'},
        dynamic={'time', 'lon', 'lat', 'sog', 'cog'},
    )


def _make_erratic_track(n=80, mmsi=888):
    '''Vessel alternating between high and zero speed — erratic profile.'''
    rng = np.random.default_rng(11)
    t0 = 1_620_000_000
    times = np.array([t0 + i * 10 for i in range(n)], dtype=np.uint32)
    lons = np.linspace(-63.5, -63.0, n, dtype=np.float32)
    lats = np.linspace(44.5, 44.8, n, dtype=np.float32)
    sog = np.where(np.arange(n) % 2 == 0, 0.0, 40.0).astype(np.float32)
    cog = np.full(n, 90, dtype=np.uint32)
    return dict(
        mmsi=mmsi, ship_type_txt='cargo',
        time=times, lon=lons, lat=lats, sog=sog, cog=cog,
        static={'mmsi', 'ship_type_txt'},
        dynamic={'time', 'lon', 'lat', 'sog', 'cog'},
    )


def _make_course_reversal_track(n=60, mmsi=777):
    '''Vessel that abruptly reverses course at mid-track.'''
    t0 = 1_620_000_000
    times = np.array([t0 + i * 10 for i in range(n)], dtype=np.uint32)
    lons = np.linspace(-63.5, -63.0, n, dtype=np.float32)
    lats = np.linspace(44.5, 44.8, n, dtype=np.float32)
    sog = np.full(n, 12.0, dtype=np.float32)
    cog = np.array([90 if i < n // 2 else 270 for i in range(n)], dtype=np.uint32)
    return dict(
        mmsi=mmsi, ship_type_txt='tanker',
        time=times, lon=lons, lat=lats, sog=sog, cog=cog,
        static={'mmsi', 'ship_type_txt'},
        dynamic={'time', 'lon', 'lat', 'sog', 'cog'},
    )


def _normal_corpus(n_tracks=40, n_pings=80):
    return [_make_track(n=n_pings, jitter=0.001) for _ in range(n_tracks)]


# ---------------------------------------------------------------------------
# Unit tests — feature extraction
# ---------------------------------------------------------------------------

class TestFeatureExtraction:
    def test_output_shape(self):
        track = _make_track(n=50)
        feats = _extract_features(track)
        assert feats.shape == (49, N_FEATURES)

    def test_dtype_float32(self):
        feats = _extract_features(_make_track(n=20))
        assert feats.dtype == np.float32

    def test_all_finite(self):
        feats = _extract_features(_make_track(n=40))
        assert np.all(np.isfinite(feats))

    def test_single_ping_returns_empty(self):
        track = _make_track(n=1)
        feats = _extract_features(track)
        assert feats.shape == (0, N_FEATURES)

    def test_unavailable_cog_handled(self):
        # COG > 360 means unavailable in AIS — should not crash or produce NaN
        track = _make_track(n=20)
        track['cog'] = np.full(20, 511, dtype=np.uint32)   # 511 = unavailable
        feats = _extract_features(track)
        assert np.all(np.isfinite(feats))

    def test_log_distance_positive(self):
        feats = _extract_features(_make_track(n=30))
        assert np.all(feats[:, 0] >= 0)   # log1p(dist) >= 0

    def test_cog_unit_circle(self):
        feats = _extract_features(_make_track(n=30))
        sin_cog = feats[:, 3]
        cos_cog = feats[:, 4]
        # sin² + cos² = 1 for valid angles
        np.testing.assert_allclose(sin_cog ** 2 + cos_cog ** 2, 1.0, atol=1e-5)


# ---------------------------------------------------------------------------
# Unit tests — sliding windows
# ---------------------------------------------------------------------------

class TestSlidingWindows:
    def test_shape_exact_multiple(self):
        feats = np.zeros((64, N_FEATURES), dtype=np.float32)
        windows, starts = _sliding_windows(feats, window=32, stride=16)
        assert windows.shape[1] == 32
        assert windows.shape[2] == N_FEATURES

    def test_short_track_padded(self):
        feats = np.ones((10, N_FEATURES), dtype=np.float32)
        windows, starts = _sliding_windows(feats, window=32, stride=16)
        assert windows.shape == (1, 32, N_FEATURES)
        # Original data preserved, rest zero
        assert np.all(windows[0, :10] == 1.0)
        assert np.all(windows[0, 10:] == 0.0)

    def test_window_scores_mapping(self):
        scores = np.array([0.8, 0.2])
        starts = np.array([0, 8])
        result = _window_scores_to_ping_scores(scores, starts, window_size=16, n_gaps=24)
        assert result.shape == (24,)
        assert np.all(np.isfinite(result))
        # Overlap region [8:16] should be averaged
        assert result[8] == pytest.approx((0.8 + 0.2) / 2, abs=1e-5)


# ---------------------------------------------------------------------------
# Unit tests — AnomalyEvent
# ---------------------------------------------------------------------------

class TestAnomalyEvent:
    def test_duration(self):
        ev = AnomalyEvent(
            mmsi=1, start_epoch=0, end_epoch=7200,
            start_lat=44.5, start_lon=-63.5,
            end_lat=44.6, end_lon=-63.4,
            anomaly_score=0.9, anomaly_type='loitering', n_pings=10,
        )
        assert ev.duration_s == pytest.approx(7200.0)
        assert ev.duration_hours == pytest.approx(2.0)

    def test_datetime_properties(self):
        ev = AnomalyEvent(
            mmsi=1, start_epoch=0, end_epoch=3600,
            start_lat=0.0, start_lon=0.0,
            end_lat=0.0, end_lon=0.0,
            anomaly_score=0.7,
        )
        assert ev.start_dt == datetime(1970, 1, 1, 0, 0, 0)

    def test_repr_contains_key_info(self):
        ev = AnomalyEvent(
            mmsi=123, start_epoch=1_620_000_000, end_epoch=1_620_007_200,
            start_lat=44.5, start_lon=-63.5,
            end_lat=44.6, end_lon=-63.4,
            anomaly_score=0.85, anomaly_type='loitering',
        )
        r = repr(ev)
        assert '123' in r
        assert 'loitering' in r
        assert '0.85' in r


# ---------------------------------------------------------------------------
# Unit tests — anomaly type inference
# ---------------------------------------------------------------------------

class TestAnomalyTypeInference:
    def test_loitering_detection(self):
        # Low distance, low sog
        feats = np.zeros((20, N_FEATURES), dtype=np.float32)
        feats[:, 0] = np.log1p(5)    # 5 m dist
        feats[:, 2] = 0.2            # 0.2 knots
        assert _infer_anomaly_type(feats) == 'loitering'

    def test_course_reversal_detection(self):
        feats = np.zeros((20, N_FEATURES), dtype=np.float32)
        feats[:, 2] = 12.0           # 12 knots
        feats[:, 5] = 0.9            # large Δcog
        assert _infer_anomaly_type(feats) == 'course_reversal'

    def test_empty_returns_unknown(self):
        feats = np.empty((0, N_FEATURES), dtype=np.float32)
        assert _infer_anomaly_type(feats) == 'unknown'


# ---------------------------------------------------------------------------
# Unit tests — TrajectoryAnomalyDetector (statistical mode)
# ---------------------------------------------------------------------------

class TestStatisticalDetector:
    def test_repr_unfitted(self):
        d = TrajectoryAnomalyDetector()
        assert 'unfitted' in repr(d)

    def test_annotate_adds_keys(self):
        track = _make_track(n=40)
        d = TrajectoryAnomalyDetector()
        result = d.annotate(track)
        assert 'anomaly_scores' in result
        assert 'anomaly_events' in result

    def test_scores_shape(self):
        n = 50
        track = _make_track(n=n)
        d = TrajectoryAnomalyDetector()
        result = d.annotate(track)
        assert result['anomaly_scores'].shape == (n - 1,)

    def test_scores_in_range(self):
        track = _make_track(n=60)
        d = TrajectoryAnomalyDetector()
        result = d.annotate(track)
        assert np.all(result['anomaly_scores'] >= 0)
        assert np.all(result['anomaly_scores'] <= 1)

    def test_single_ping_track(self):
        track = _make_track(n=1)
        d = TrajectoryAnomalyDetector()
        result = d.annotate(track)
        assert len(result['anomaly_scores']) == 0
        assert result['anomaly_events'] == []

    def test_dynamic_keys_registered(self):
        track = _make_track(n=30)
        d = TrajectoryAnomalyDetector()
        result = d.annotate(track)
        assert 'anomaly_scores' in result['dynamic']

    def test_fit_returns_self(self):
        d = TrajectoryAnomalyDetector()
        result = d.fit(_normal_corpus())
        assert result is d

    def test_fit_marks_fitted(self):
        d = TrajectoryAnomalyDetector()
        d.fit(_normal_corpus())
        assert d._fitted is True
        assert 'fitted' in repr(d)

    def test_fitted_scores_normal_lower_than_anomalous(self):
        '''After fitting on normal tracks, the peak anomaly score on a normal
        track should be lower than on an extreme erratic-speed track.
        Isolation Forest operates per-gap-feature; erratic speed (alternating
        0 / 40 knots) produces feature vectors far outside the training
        distribution and should receive higher maximum scores.'''
        d = TrajectoryAnomalyDetector(mode='statistical')
        d.fit(_normal_corpus(n_tracks=60))

        normal = d.annotate(_make_track(n=60))
        erratic = d.annotate(_make_erratic_track(n=60))

        assert np.max(normal['anomaly_scores']) <= np.max(erratic['anomaly_scores'])

    def test_flag_anomalies_generator(self):
        tracks = [_make_track(n=40), _make_loitering_track(n=40)]
        d = TrajectoryAnomalyDetector()
        results = list(d.flag_anomalies(tracks, threshold=0.5))
        assert len(results) == 2
        assert all('anomaly_events' in t for t in results)

    def test_module_level_flag_anomalies_no_detector(self):
        tracks = [_make_track(n=30)]
        results = list(flag_anomalies(tracks))
        assert 'anomaly_events' in results[0]

    def test_anomaly_events_have_correct_types(self):
        d = TrajectoryAnomalyDetector()
        d.fit(_normal_corpus())
        result = d.annotate(_make_loitering_track(n=80), threshold=0.3)
        for ev in result['anomaly_events']:
            assert isinstance(ev, AnomalyEvent)
            assert 0.0 <= ev.anomaly_score <= 1.0
            assert ev.n_pings >= 1

    def test_save_load_roundtrip(self, tmp_path):
        d = TrajectoryAnomalyDetector(mode='statistical')
        d.fit(_normal_corpus())
        path = str(tmp_path / 'stat_model.pkl')
        d.save(path)

        loaded = TrajectoryAnomalyDetector.load(path)
        assert loaded._fitted is True
        assert loaded.mode == 'statistical'

        track = _make_track(n=40)
        s1 = d.annotate(dict(**track))['anomaly_scores']
        s2 = loaded.annotate(dict(**track))['anomaly_scores']
        np.testing.assert_allclose(s1, s2, rtol=1e-4)

    def test_threshold_controls_events(self):
        # A stricter threshold flags fewer total pings (not necessarily fewer
        # events, since a strict cut can fragment one big run into many small ones)
        d = TrajectoryAnomalyDetector()
        track_strict = _make_loitering_track(n=80)
        track_loose = _make_loitering_track(n=80)
        r_strict = d.annotate(track_strict, threshold=0.99)
        r_loose = d.annotate(track_loose, threshold=0.01)
        pings_strict = sum(e.n_pings for e in r_strict['anomaly_events'])
        pings_loose = sum(e.n_pings for e in r_loose['anomaly_events'])
        assert pings_strict <= pings_loose


# ---------------------------------------------------------------------------
# Unit tests — LSTM mode
# ---------------------------------------------------------------------------

class TestLSTMDetector:
    def test_fit_creates_lstm(self):
        d = TrajectoryAnomalyDetector(mode='lstm', window_size=16, stride=8)
        d.fit(_normal_corpus(n_tracks=20, n_pings=60), epochs=2)
        assert d._lstm is not None
        assert d._fitted is True

    def test_lstm_scores_shape(self):
        n = 50
        d = TrajectoryAnomalyDetector(mode='lstm', window_size=16, stride=8)
        d.fit(_normal_corpus(n_tracks=20, n_pings=60), epochs=2)
        result = d.annotate(_make_track(n=n))
        assert result['anomaly_scores'].shape == (n - 1,)

    def test_lstm_scores_in_range(self):
        d = TrajectoryAnomalyDetector(mode='lstm', window_size=16, stride=8)
        d.fit(_normal_corpus(n_tracks=20, n_pings=60), epochs=2)
        result = d.annotate(_make_erratic_track(n=60))
        scores = result['anomaly_scores']
        assert np.all(scores >= 0)
        assert np.all(scores <= 1)

    def test_lstm_save_load_roundtrip(self, tmp_path):
        d = TrajectoryAnomalyDetector(mode='lstm', window_size=16, stride=8)
        d.fit(_normal_corpus(n_tracks=20, n_pings=60), epochs=2)
        path = str(tmp_path / 'lstm_model.pkl')
        d.save(path)

        loaded = TrajectoryAnomalyDetector.load(path)
        assert loaded.mode == 'lstm'
        assert loaded._lstm is not None

        track = _make_track(n=50)
        s1 = d.annotate(dict(**track))['anomaly_scores']
        s2 = loaded.annotate(dict(**track))['anomaly_scores']
        np.testing.assert_allclose(s1, s2, atol=1e-4)

    def test_lstm_anomalous_scores_higher_than_normal(self):
        d = TrajectoryAnomalyDetector(mode='lstm', window_size=16, stride=8)
        d.fit(_normal_corpus(n_tracks=40, n_pings=60), epochs=5)

        normal = d.annotate(_make_track(n=60))
        anomalous = d.annotate(_make_erratic_track(n=60))

        assert np.mean(anomalous['anomaly_scores']) > np.mean(normal['anomaly_scores'])

    def test_unfitted_lstm_falls_back_gracefully(self):
        d = TrajectoryAnomalyDetector(mode='lstm', window_size=16, stride=8)
        # No fit() called — should fall back to statistical scoring without crash
        result = d.annotate(_make_track(n=40))
        assert 'anomaly_scores' in result

    def test_lstm_model_architecture(self):
        from aisdb.anomaly.lstm_anomaly import _LSTMAutoencoder
        model = _LSTMAutoencoder(input_size=N_FEATURES, hidden_size=32,
                                 latent_size=8, n_layers=1, seq_len=16)
        x = torch.randn(4, 16, N_FEATURES)
        out = model(x)
        assert out.shape == (4, 16, N_FEATURES)

    def test_reconstruction_error_shape(self):
        from aisdb.anomaly.lstm_anomaly import _LSTMAutoencoder
        model = _LSTMAutoencoder(input_size=N_FEATURES, hidden_size=32,
                                 latent_size=8, n_layers=1, seq_len=DEFAULT_WINDOW)
        x = torch.randn(8, DEFAULT_WINDOW, N_FEATURES)
        err = model.reconstruction_error(x)
        assert err.shape == (8,)
        assert torch.all(err >= 0)

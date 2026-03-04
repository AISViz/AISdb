'''Tests for aisdb.anomaly.dark_vessel — dark vessel / AIS gap detection.'''

import os
from datetime import datetime, timedelta

import numpy as np
import pytest

from aisdb import SQLiteDBConn, DBQuery, TrackGen, decode_msgs
from aisdb.database import sqlfcn_callbacks
from aisdb.anomaly.dark_vessel import (
    DarkVesselDetector,
    DarkEvent,
    flag_dark_gaps,
    _normalise_ship_type,
    _gap_features,
    _rule_based_scores,
    DARK_THRESHOLDS,
)
from aisdb.tests.create_testing_data import sample_database_file


# ---------------------------------------------------------------------------
# Synthetic track builder
# ---------------------------------------------------------------------------

def _make_track(ping_times, ship_type_txt='cargo', mmsi=123456789):
    '''Build a minimal AISdb track dict from a list of epoch-second timestamps.'''
    n = len(ping_times)
    time = np.array(ping_times, dtype=np.uint32)
    lon = np.linspace(-63.5, -63.0, n, dtype=np.float32)
    lat = np.linspace(44.5, 44.8, n, dtype=np.float32)
    return dict(
        mmsi=mmsi,
        ship_type_txt=ship_type_txt,
        time=time,
        lon=lon,
        lat=lat,
        sog=np.full(n, 8.0, dtype=np.float32),
        cog=np.full(n, 90, dtype=np.uint32),
        static={'mmsi', 'ship_type_txt'},
        dynamic={'time', 'lon', 'lat', 'sog', 'cog'},
    )


def _regular_pings(n=100, interval_s=10, start=1_620_000_000):
    '''n evenly-spaced pings at interval_s seconds.'''
    return list(range(start, start + n * interval_s, interval_s))


def _inject_gap(times, after_index, gap_s):
    '''Insert a gap of gap_s seconds after times[after_index].'''
    times = list(times)
    offset = gap_s - (times[after_index + 1] - times[after_index])
    for i in range(after_index + 1, len(times)):
        times[i] += offset
    return times


# ---------------------------------------------------------------------------
# Unit tests — helpers
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_normalise_ship_type_known(self):
        track = {'ship_type_txt': 'Cargo vessel'}
        assert _normalise_ship_type(track) == 'cargo'

    def test_normalise_ship_type_fallback(self):
        track = {'ship_type_txt': ''}
        assert _normalise_ship_type(track) == 'default'

    def test_normalise_ship_type_missing_key(self):
        assert _normalise_ship_type({}) == 'default'

    def test_gap_features_shape(self):
        times = np.array([0, 10, 20, 30], dtype=np.uint32)
        feats = _gap_features(times)
        assert feats.shape == (3, 1)

    def test_gap_features_single_ping(self):
        feats = _gap_features(np.array([100], dtype=np.uint32))
        assert feats.shape == (0, 1)

    def test_gap_features_log_transform(self):
        # gap of 9 seconds → log1p(9) ≈ 2.303
        times = np.array([0, 9], dtype=np.uint32)
        feats = _gap_features(times)
        np.testing.assert_allclose(feats[0, 0], np.log1p(9), rtol=1e-5)

    def test_rule_based_scores_normal(self):
        # 10-second gaps vs 1800s cargo threshold: sigmoid centred at threshold,
        # half-width 900s → score ≈ 0.12 (well below the default 0.5 cutoff)
        times = np.array(_regular_pings(n=20, interval_s=10), dtype=np.uint32)
        scores = _rule_based_scores(times, 'cargo')
        assert scores.shape == (19,)
        assert np.all(scores < 0.5)

    def test_rule_based_scores_dark(self):
        # A 5-hour gap should score very high for any vessel type
        times = _regular_pings(n=3, interval_s=10)
        times = _inject_gap(times, after_index=0, gap_s=18000)
        scores = _rule_based_scores(np.array(times, dtype=np.uint32), 'default')
        assert scores[0] > 0.9   # the injected gap

    def test_rule_based_scores_empty(self):
        scores = _rule_based_scores(np.array([42], dtype=np.uint32), 'cargo')
        assert len(scores) == 0


# ---------------------------------------------------------------------------
# Unit tests — DarkEvent
# ---------------------------------------------------------------------------

class TestDarkEvent:
    def test_duration_hours(self):
        ev = DarkEvent(
            mmsi=1, start_epoch=0, end_epoch=3600,
            duration_s=3600.0,
            lat_before=0.0, lon_before=0.0,
            lat_after=0.0, lon_after=0.0,
            gap_score=0.9, ship_type='cargo',
        )
        assert ev.duration_hours == pytest.approx(1.0)

    def test_datetime_properties(self):
        ev = DarkEvent(
            mmsi=1, start_epoch=0, end_epoch=3600,
            duration_s=3600.0,
            lat_before=0.0, lon_before=0.0,
            lat_after=0.0, lon_after=0.0,
            gap_score=0.9,
        )
        assert ev.start_dt == datetime(1970, 1, 1, 0, 0, 0)
        assert ev.end_dt == datetime(1970, 1, 1, 1, 0, 0)

    def test_repr(self):
        ev = DarkEvent(
            mmsi=123456789, start_epoch=1_620_000_000, end_epoch=1_620_003_600,
            duration_s=3600.0,
            lat_before=44.5, lon_before=-63.5,
            lat_after=44.6, lon_after=-63.4,
            gap_score=0.95,
        )
        assert 'DarkEvent' in repr(ev)
        assert '123456789' in repr(ev)


# ---------------------------------------------------------------------------
# Unit tests — DarkVesselDetector (rule-based, no HMM)
# ---------------------------------------------------------------------------

class TestDarkVesselDetectorRuleBased:
    def test_annotate_adds_keys(self):
        times = _regular_pings(n=20, interval_s=10)
        track = _make_track(times)
        detector = DarkVesselDetector()
        result = detector.annotate(track)
        assert 'gap_durations' in result
        assert 'gap_scores' in result
        assert 'dark_events' in result

    def test_annotate_shape_consistency(self):
        n = 30
        times = _regular_pings(n=n, interval_s=10)
        track = _make_track(times)
        detector = DarkVesselDetector()
        result = detector.annotate(track)
        assert len(result['gap_durations']) == n - 1
        assert len(result['gap_scores']) == n - 1

    def test_no_dark_events_on_regular_pings(self):
        times = _regular_pings(n=50, interval_s=10)
        track = _make_track(times, ship_type_txt='cargo')
        detector = DarkVesselDetector()
        result = detector.annotate(track, threshold=0.5)
        assert result['dark_events'] == []

    def test_dark_event_detected_on_large_gap(self):
        times = _regular_pings(n=40, interval_s=10)
        # Inject a 4-hour gap after ping 20
        times = _inject_gap(times, after_index=20, gap_s=14400)
        track = _make_track(times, ship_type_txt='cargo')
        detector = DarkVesselDetector()
        result = detector.annotate(track, threshold=0.5)
        assert len(result['dark_events']) >= 1
        event = result['dark_events'][0]
        assert isinstance(event, DarkEvent)
        assert event.duration_s == pytest.approx(14400.0, abs=1.0)
        assert event.gap_score > 0.5
        assert event.mmsi == 123456789

    def test_single_ping_track(self):
        track = _make_track([1_620_000_000], ship_type_txt='tanker')
        detector = DarkVesselDetector()
        result = detector.annotate(track)
        assert len(result['gap_durations']) == 0
        assert result['dark_events'] == []

    def test_dynamic_keys_updated(self):
        times = _regular_pings(n=10)
        track = _make_track(times)
        detector = DarkVesselDetector()
        result = detector.annotate(track)
        assert 'gap_durations' in result['dynamic']
        assert 'gap_scores' in result['dynamic']

    def test_flag_dark_gaps_generator(self):
        tracks = [
            _make_track(_inject_gap(_regular_pings(n=30), 15, 7200), 'fishing'),
            _make_track(_regular_pings(n=30, interval_s=5), 'tanker'),
        ]
        detector = DarkVesselDetector()
        results = list(detector.flag_dark_gaps(tracks, threshold=0.5))
        assert len(results) == 2
        assert all('dark_events' in t for t in results)

    def test_module_level_flag_dark_gaps_no_detector(self):
        tracks = [_make_track(_regular_pings(n=20))]
        results = list(flag_dark_gaps(tracks))
        assert len(results) == 1
        assert 'dark_events' in results[0]

    def test_threshold_sensitivity(self):
        times = _inject_gap(_regular_pings(n=40), 20, 3600)
        track_a = _make_track(times, 'cargo')
        track_b = _make_track(times, 'cargo')
        detector = DarkVesselDetector()
        result_strict = detector.annotate(track_a, threshold=0.99)
        result_loose = detector.annotate(track_b, threshold=0.01)
        assert len(result_strict['dark_events']) <= len(result_loose['dark_events'])


# ---------------------------------------------------------------------------
# Unit tests — DarkVesselDetector with HMM training
# ---------------------------------------------------------------------------

class TestDarkVesselDetectorHMM:
    def _make_training_corpus(self, n_tracks=30, n_pings=200):
        '''Generate synthetic training tracks with occasional dark gaps.'''
        rng = np.random.default_rng(42)
        tracks = []
        t0 = 1_620_000_000
        for i in range(n_tracks):
            times = list(range(t0, t0 + n_pings * 10, 10))
            # Randomly inject 0–2 gaps per track
            for _ in range(rng.integers(0, 3)):
                idx = rng.integers(1, n_pings - 2)
                gap = int(rng.uniform(1800, 7200))
                times = _inject_gap(times, after_index=idx, gap_s=gap)
            tracks.append(_make_track(times, ship_type_txt='cargo'))
        return tracks

    def test_fit_returns_self(self):
        detector = DarkVesselDetector()
        corpus = self._make_training_corpus()
        result = detector.fit(corpus)
        assert result is detector

    def test_fit_creates_model(self):
        detector = DarkVesselDetector()
        corpus = self._make_training_corpus()
        detector.fit(corpus)
        assert 'cargo' in detector._models
        assert detector._fitted is True

    def test_hmm_detects_injected_gap(self):
        detector = DarkVesselDetector()
        corpus = self._make_training_corpus(n_tracks=60)
        detector.fit(corpus)

        times = _inject_gap(_regular_pings(n=50, interval_s=10), 25, 10800)
        track = _make_track(times, 'cargo')
        result = detector.annotate(track, threshold=0.5)
        assert len(result['dark_events']) >= 1

    def test_repr_shows_fitted_types(self):
        detector = DarkVesselDetector()
        corpus = self._make_training_corpus()
        detector.fit(corpus)
        assert 'cargo' in repr(detector)

    def test_save_load_roundtrip(self, tmp_path):
        detector = DarkVesselDetector()
        corpus = self._make_training_corpus()
        detector.fit(corpus)

        model_path = str(tmp_path / 'test_model.pkl')
        detector.save(model_path)

        loaded = DarkVesselDetector.load(model_path)
        assert loaded._fitted is True
        assert set(loaded._models.keys()) == set(detector._models.keys())

        # Inference should produce identical scores after round-trip
        times = _inject_gap(_regular_pings(n=30), 15, 5400)
        track_a = _make_track(times, 'cargo')
        track_b = _make_track(times, 'cargo')
        res_a = detector.annotate(track_a, threshold=0.5)
        res_b = loaded.annotate(track_b, threshold=0.5)
        np.testing.assert_allclose(res_a['gap_scores'], res_b['gap_scores'], rtol=1e-4)

    def test_unknown_type_falls_back_to_rule_based(self):
        detector = DarkVesselDetector()
        # Fit only on cargo — unknown type should fall back gracefully
        corpus = self._make_training_corpus()
        detector.fit(corpus)

        times = _inject_gap(_regular_pings(n=30), 15, 7200)
        track = _make_track(times, ship_type_txt='submarine')   # not a known type
        result = detector.annotate(track, threshold=0.5)
        # Should not raise; may or may not flag depending on fallback
        assert 'dark_events' in result


# ---------------------------------------------------------------------------
# Integration test — real AISdb test data
# ---------------------------------------------------------------------------

class TestIntegrationWithAISdb:
    @pytest.mark.skipif(
        __import__('sys').version_info >= (3, 14),
        reason='decode_msgs segfaults under Python 3.14 with pyo3 0.18.3 '
               '(pre-existing upstream issue unrelated to this module)',
    )
    def test_pipeline_with_real_data(self, tmpdir):
        dbpath = os.path.join(tmpdir, 'test_dark_vessel.db')
        months = sample_database_file(dbpath)
        start = datetime(int(months[0][:4]), int(months[0][4:6]), 1)
        end = start + timedelta(weeks=4)

        detector = DarkVesselDetector()

        with SQLiteDBConn(dbpath) as dbconn:
            qry = DBQuery(
                dbconn=dbconn,
                start=start,
                end=end,
                callback=sqlfcn_callbacks.in_timerange_validmmsi,
            )
            tracks = TrackGen(qry.gen_qry(), decimate=True)
            flagged = list(detector.flag_dark_gaps(tracks, threshold=0.5))

        assert len(flagged) > 0
        for track in flagged:
            assert 'gap_durations' in track
            assert 'gap_scores' in track
            assert 'dark_events' in track
            assert len(track['gap_durations']) == len(track['time']) - 1 or len(track['time']) < 2
            for event in track['dark_events']:
                assert isinstance(event, DarkEvent)
                assert 0.0 <= event.gap_score <= 1.0
                assert event.duration_s > 0

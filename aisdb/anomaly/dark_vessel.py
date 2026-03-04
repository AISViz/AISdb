'''Dark Vessel Detection — AIS Transmission Gap Analysis

Detects suspicious gaps in AIS transmissions using a 2-state Gaussian HMM
trained on normal vessel behaviour.  Vessels that suppress their AIS
transponder while still operating ("going dark") may be engaged in illegal
fishing, sanctions evasion, smuggling, or other activity that violates
maritime law.

Gap reporting-rate priors are informed by ITU-R M.1371-5:
  - Class A underway  :  2 – 10 s
  - Class A at anchor :  3 min
  - Class B           :  30 s – 3 min
  - Fishing vessels   :  3 min  (often Class A)

Two-state HMM:
  state 0 — NORMAL  : ping interval consistent with vessel type & area
  state 1 — DARK    : gap significantly exceeds expected reporting rate

The model is trained per vessel type from historical TrackGen output.
When insufficient training data exist for a type the module falls back to
a logistic rule-based scorer calibrated to the ITU thresholds above.

Typical pipeline usage::

    from aisdb.anomaly.dark_vessel import DarkVesselDetector, flag_dark_gaps

    detector = DarkVesselDetector()
    detector.fit(training_tracks)               # TrackGen generator
    detector.save('dark_vessel_model.pkl')

    detector = DarkVesselDetector.load('dark_vessel_model.pkl')
    for track in flag_dark_gaps(live_tracks, detector, threshold=0.6):
        for event in track['dark_events']:
            print(event)
'''

import logging
import pickle
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Generator, Iterable, List, Optional

import numpy as np
from hmmlearn.hmm import GaussianHMM

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ITU-R M.1371 nominal reporting intervals (seconds) — used as HMM priors
# ---------------------------------------------------------------------------

#: Expected ping interval (seconds) per vessel category.
REPORTING_INTERVALS: Dict[str, float] = {
    'fishing':    180.0,
    'tanker':      10.0,
    'cargo':       10.0,
    'passenger':    5.0,
    'tug':         10.0,
    'default':     10.0,
}

#: Gap duration (seconds) beyond which a gap is flagged by the rule-based
#: fallback scorer (used when no trained HMM is available).
DARK_THRESHOLDS: Dict[str, float] = {
    'fishing':   3600.0,   # 1 hour
    'tanker':    1800.0,   # 30 min
    'cargo':     1800.0,
    'passenger':  900.0,   # 15 min
    'default':   1800.0,
}


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class DarkEvent:
    '''A detected AIS transmission gap that exceeds expected behaviour.

    Attributes:
        mmsi:         Maritime Mobile Service Identity of the vessel.
        start_epoch:  Unix epoch seconds — last ping before the gap.
        end_epoch:    Unix epoch seconds — first ping after the gap.
        duration_s:   Gap length in seconds.
        lat_before:   Latitude of last known position before gap.
        lon_before:   Longitude of last known position before gap.
        lat_after:    Latitude of first position after gap.
        lon_after:    Longitude of first position after gap.
        gap_score:    Darkness probability in [0, 1] (1 = very likely dark).
        ship_type:    Normalised vessel type string used for scoring.
    '''
    mmsi: int
    start_epoch: int
    end_epoch: int
    duration_s: float
    lat_before: float
    lon_before: float
    lat_after: float
    lon_after: float
    gap_score: float
    ship_type: str = 'unknown'

    @property
    def duration_hours(self) -> float:
        return self.duration_s / 3600.0

    @property
    def start_dt(self) -> datetime:
        return datetime.fromtimestamp(self.start_epoch, tz=timezone.utc).replace(tzinfo=None)

    @property
    def end_dt(self) -> datetime:
        return datetime.fromtimestamp(self.end_epoch, tz=timezone.utc).replace(tzinfo=None)

    def __repr__(self) -> str:
        return (
            f'DarkEvent(mmsi={self.mmsi}, '
            f'start={self.start_dt.isoformat()}, '
            f'duration={self.duration_hours:.2f}h, '
            f'score={self.gap_score:.3f})'
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalise_ship_type(track: dict) -> str:
    '''Return the normalised ship-type key for a track dict.'''
    raw = track.get('ship_type_txt', '') or track.get('ship_type', '') or ''
    raw = str(raw).lower()
    for key in REPORTING_INTERVALS:
        if key in raw:
            return key
    return 'default'


def _gap_features(time_arr: np.ndarray) -> np.ndarray:
    '''Compute per-gap log-duration features from a timestamp array.

    Returns shape ``(n_pings - 1, 1)`` using ``log1p(seconds)`` to
    compress the heavy tail of gap-duration distributions.
    '''
    if len(time_arr) < 2:
        return np.empty((0, 1), dtype=np.float64)
    gaps = np.diff(time_arr.astype(np.float64))
    gaps = np.maximum(gaps, 0.0)   # guard against duplicate / unsorted timestamps
    return np.log1p(gaps).reshape(-1, 1)


def _rule_based_scores(time_arr: np.ndarray, ship_type: str) -> np.ndarray:
    '''Logistic rule-based darkness scorer (no HMM required).

    Returns a float array in [0, 1] for each gap — 0 = normal, 1 = dark.
    The sigmoid is centred at the type-specific threshold with a half-width
    of 50 % of that threshold, giving a smooth ramp rather than a hard cut.
    '''
    threshold = DARK_THRESHOLDS.get(ship_type, DARK_THRESHOLDS['default'])
    if len(time_arr) < 2:
        return np.array([], dtype=np.float64)
    gaps = np.diff(time_arr.astype(np.float64))
    gaps = np.maximum(gaps, 0.0)
    x = (gaps - threshold) / (threshold * 0.5)
    return 1.0 / (1.0 + np.exp(-x))


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

class DarkVesselDetector:
    '''Detects AIS transmission gaps using a 2-state Gaussian HMM.

    One model is trained per vessel type.  If insufficient training data
    are available for a given type the rule-based logistic scorer is used
    transparently as a fallback.

    Training example::

        detector = DarkVesselDetector()
        detector.fit(TrackGen(rowgen, decimate=True))
        detector.save('dark_vessel_model.pkl')

    Inference example::

        detector = DarkVesselDetector.load('dark_vessel_model.pkl')
        for track in detector.flag_dark_gaps(tracks, threshold=0.6):
            for event in track['dark_events']:
                print(event)
    '''

    #: Number of HMM states.
    N_STATES = 2
    NORMAL_STATE = 0
    DARK_STATE = 1

    #: Minimum number of training gaps required to fit a model for a type.
    MIN_GAPS = 50

    def __init__(self) -> None:
        self._models: Dict[str, GaussianHMM] = {}
        self._fitted: bool = False

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def fit(self, tracks: Iterable[dict]) -> 'DarkVesselDetector':
        '''Fit one GaussianHMM per vessel type from a collection of tracks.

        args:
            tracks: iterable of AISdb track dicts (e.g. from TrackGen).

        returns:
            self  — allows method chaining.
        '''
        corpus: Dict[str, List[np.ndarray]] = {}
        for track in tracks:
            stype = _normalise_ship_type(track)
            feats = _gap_features(track['time'])
            if feats.shape[0] == 0:
                continue
            corpus.setdefault(stype, []).append(feats)

        for stype, feat_list in corpus.items():
            all_feats = np.vstack(feat_list)
            n_gaps = all_feats.shape[0]
            if n_gaps < self.MIN_GAPS:
                logger.info(
                    'Skipping HMM for "%s": only %d gaps (need %d)',
                    stype, n_gaps, self.MIN_GAPS,
                )
                continue

            model = GaussianHMM(
                n_components=self.N_STATES,
                covariance_type='full',
                n_iter=200,
                random_state=42,
            )
            # Strong self-loop prior — the vessel spends most time in NORMAL
            model.startprob_ = np.array([0.99, 0.01])
            model.transmat_ = np.array([[0.99, 0.01],
                                        [0.10, 0.90]])
            try:
                model.fit(all_feats)
                # Ensure state index 1 always represents the DARK (higher mean) state
                if model.means_.flatten()[0] > model.means_.flatten()[1]:
                    for attr in ('means_', 'covars_'):
                        arr = getattr(model, attr)
                        arr[[0, 1]] = arr[[1, 0]]
                    model.transmat_[:, [0, 1]] = model.transmat_[:, [1, 0]]
                    model.transmat_[[0, 1], :] = model.transmat_[[1, 0], :]
                    model.startprob_[[0, 1]] = model.startprob_[[1, 0]]

                self._models[stype] = model
                logger.info('Fitted HMM for "%s" on %d gaps', stype, n_gaps)
            except Exception as exc:
                logger.warning('HMM fit failed for "%s": %s', stype, exc)

        self._fitted = bool(self._models)
        return self

    # ------------------------------------------------------------------
    # Inference
    # ------------------------------------------------------------------

    def _score_gaps(self, time_arr: np.ndarray, ship_type: str) -> np.ndarray:
        '''Return per-gap darkness scores in [0, 1].

        Uses ``P(state=DARK | observations)`` from the fitted HMM when
        available, otherwise falls back to the rule-based logistic scorer.
        '''
        feats = _gap_features(time_arr)
        if feats.shape[0] == 0:
            return np.array([], dtype=np.float64)

        model = self._models.get(ship_type) or self._models.get('default')
        if model is not None:
            try:
                _, posteriors = model.score_samples(feats)
                return posteriors[:, self.DARK_STATE]
            except Exception as exc:
                logger.debug('HMM score_samples failed (%s) — using rule-based', exc)

        return _rule_based_scores(time_arr, ship_type)

    def annotate(self, track: dict, threshold: float = 0.5) -> dict:
        '''Annotate a single track dict in-place with gap scores and events.

        Adds the following keys to the track dict:

        ``gap_durations`` (np.ndarray)
            Seconds between consecutive pings, shape ``(n_pings - 1,)``.

        ``gap_scores`` (np.ndarray)
            Darkness probability per gap in [0, 1], same shape.

        ``dark_events`` (list[DarkEvent])
            One entry per gap whose score is >= *threshold*.

        ``gap_durations`` and ``gap_scores`` are also registered in
        ``track['dynamic']`` so downstream AISdb tools treat them
        as per-ping arrays.

        args:
            track:     AISdb track dict.
            threshold: Darkness score cutoff in [0, 1].

        returns:
            The annotated track dict.
        '''
        time = track['time']
        if len(time) < 2:
            track['gap_durations'] = np.array([], dtype=np.float64)
            track['gap_scores'] = np.array([], dtype=np.float64)
            track['dark_events'] = []
            return track

        stype = _normalise_ship_type(track)
        gaps = np.diff(time.astype(np.float64))
        gaps = np.maximum(gaps, 0.0)
        scores = self._score_gaps(time, stype)

        events: List[DarkEvent] = []
        for i in np.where(scores >= threshold)[0]:
            events.append(DarkEvent(
                mmsi=int(track.get('mmsi', 0)),
                start_epoch=int(time[i]),
                end_epoch=int(time[i + 1]),
                duration_s=float(gaps[i]),
                lat_before=float(track['lat'][i]),
                lon_before=float(track['lon'][i]),
                lat_after=float(track['lat'][i + 1]),
                lon_after=float(track['lon'][i + 1]),
                gap_score=float(scores[i]),
                ship_type=stype,
            ))

        track['gap_durations'] = gaps
        track['gap_scores'] = scores
        track['dark_events'] = events
        track['dynamic'] = set(track.get('dynamic', set())) | {
            'gap_durations', 'gap_scores'
        }
        return track

    def flag_dark_gaps(
        self,
        tracks: Iterable[dict],
        threshold: float = 0.5,
    ) -> Generator[dict, None, None]:
        '''Generator that annotates each track with dark gap information.

        Drop-in pipeline filter compatible with TrackGen output::

            tracks = TrackGen(rowgen, decimate=True)
            tracks = detector.flag_dark_gaps(tracks, threshold=0.6)
            for track in tracks:
                for event in track['dark_events']:
                    print(event)

        args:
            tracks:    AISdb track generator.
            threshold: Darkness score cutoff in [0, 1].

        yields:
            Track dicts with added keys: ``gap_durations``, ``gap_scores``,
            ``dark_events``.
        '''
        for track in tracks:
            yield self.annotate(track, threshold=threshold)

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        '''Save fitted models to *path* (pickle).'''
        with open(path, 'wb') as fh:
            pickle.dump({'models': self._models, 'fitted': self._fitted}, fh)
        logger.info('Saved DarkVesselDetector → %s', path)

    @classmethod
    def load(cls, path: str) -> 'DarkVesselDetector':
        '''Load a previously saved DarkVesselDetector from *path*.'''
        with open(path, 'rb') as fh:
            state = pickle.load(fh)
        obj = cls()
        obj._models = state['models']
        obj._fitted = state['fitted']
        logger.info('Loaded DarkVesselDetector ← %s', path)
        return obj

    def __repr__(self) -> str:
        types = list(self._models) if self._models else ['rule-based']
        return f'DarkVesselDetector(fitted_types={types})'


# ---------------------------------------------------------------------------
# Module-level pipeline function
# ---------------------------------------------------------------------------

def flag_dark_gaps(
    tracks: Iterable[dict],
    detector: Optional[DarkVesselDetector] = None,
    threshold: float = 0.5,
) -> Generator[dict, None, None]:
    '''Annotate tracks with AIS dark gap events.

    Convenience wrapper for use in TrackGen pipelines.  If no *detector* is
    supplied a new rule-based ``DarkVesselDetector`` is created automatically
    (no training required).

    Example::

        from aisdb.anomaly.dark_vessel import flag_dark_gaps

        for track in flag_dark_gaps(TrackGen(rowgen, decimate=True)):
            for event in track['dark_events']:
                print(event)

    args:
        tracks:    AISdb track generator (TrackGen or any pipeline filter).
        detector:  Fitted :class:`DarkVesselDetector`, or ``None`` for the
                   rule-based fallback.
        threshold: Darkness score cutoff in [0, 1].

    yields:
        Annotated track dicts.
    '''
    if detector is None:
        detector = DarkVesselDetector()
    yield from detector.flag_dark_gaps(tracks, threshold=threshold)

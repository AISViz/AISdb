'''AIS Spoofing Detection — Physics-Based Signal Integrity Analysis

Detects AIS message spoofing by cross-checking transmitted positional data
against physical and kinematic constraints derived from the VHF radio
propagation model and maritime vessel performance limits.

Background
----------
AIS broadcasts on VHF channels 87B (161.975 MHz) and 88B (162.025 MHz)
using TDMA.  Because the transmission medium is well-understood, several
consistency checks are possible without extra sensors:

Kinematic impossibility
    The haversine distance between consecutive positions divided by the
    elapsed time gives an implied speed.  If this exceeds the physical
    performance envelope for the vessel type, the position is suspect.

SOG/COG inconsistency
    AIS messages carry both reported Speed Over Ground and Course Over
    Ground.  A spoofer injecting a false position without updating the
    kinematic fields will leave detectable mismatches between the
    reported values and what can be derived from consecutive positions.

Transmission interval injection
    The ITU-R M.1371 standard defines reporting intervals by vessel class
    and navigation status.  Sub-second intervals or systematically
    shorter-than-expected gaps indicate software-injected messages.

MMSI conflict
    The same MMSI appearing at two physically-separated locations within
    a time window too short to allow transit between them indicates either
    spoofing of one transmitter or MMSI duplication.

Four per-ping indicator scores are computed, each in [0, 1]:

    ``kinematic``      — implied speed vs vessel-type envelope
    ``sog_delta``      — |reported SOG − computed SOG|
    ``cog_delta``      — angular deviation between reported COG and bearing
    ``interval``       — transmission interval too short or irregular

A composite score is the weighted mean; individual SpoofEvent objects
identify the dominant indicator for each flagged segment.

Usage::

    from aisdb.anomaly.spoof_detect import SpoofingDetector, flag_spoofing

    # Per-track spoofing checks (no training required)
    for track in flag_spoofing(TrackGen(rowgen, decimate=True)):
        for event in track['spoof_events']:
            print(event)

    # Cross-track MMSI conflict detection
    detector = SpoofingDetector()
    for track in detector.check_mmsi_conflicts(tracks):
        for event in track['spoof_events']:
            if event.spoof_type == 'mmsi_conflict':
                print(event)
'''

import logging
import pickle
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Generator, Iterable, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Physical constants and vessel-type performance envelopes
# ---------------------------------------------------------------------------

#: Maximum physically credible speed (knots) per vessel type.
#: Values derived from Jane's Fighting Ships, IMO vessel class data, and
#: AIS spoofing literature (Iphar et al. 2015, Harati-Mokhtari et al. 2007).
MAX_SPEED_KNOTS: Dict[str, float] = {
    'fishing':    15.0,
    'tanker':     18.0,
    'cargo':      25.0,
    'passenger':  35.0,
    'tug':        14.0,
    'default':    50.0,   # conservative upper bound for unknown type
}

#: Nominal AIS reporting interval (seconds) per vessel type, per ITU-R M.1371.
NOMINAL_INTERVAL_S: Dict[str, float] = {
    'fishing':   180.0,
    'tanker':     10.0,
    'cargo':      10.0,
    'passenger':   5.0,
    'tug':        10.0,
    'default':    10.0,
}

#: SOG discrepancy (knots) above which the mismatch is considered suspicious.
SOG_TOLERANCE_KN: float = 5.0

#: COG angular discrepancy (degrees) above which the mismatch is flagged.
COG_TOLERANCE_DEG: float = 30.0

#: Transmission interval shorter than this (seconds) is flagged as injected.
MIN_CREDIBLE_INTERVAL_S: float = 2.0

#: MMSI conflict window: two tracks with same MMSI whose inter-position
#: speed exceeds this are flagged as conflicting.
MMSI_CONFLICT_SPEED_KN: float = 60.0


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class SpoofEvent:
    '''A detected AIS spoofing indicator at a specific position.

    Attributes:
        mmsi:              Maritime Mobile Service Identity.
        epoch:             Unix epoch seconds of the suspicious ping.
        lat:               Latitude at the suspicious ping.
        lon:               Longitude at the suspicious ping.
        spoof_score:       Composite indicator score in [0, 1].
        spoof_type:        Dominant indicator type (see module docstring).
        computed_speed_kn: Haversine-derived speed leading into this ping.
        reported_speed_kn: SOG from the AIS message.
        computed_bearing:  Bearing from previous ping (degrees, 0–360).
        reported_cog:      COG from the AIS message (degrees).
        details:           Human-readable description of the indicator.
    '''
    mmsi: int
    epoch: int
    lat: float
    lon: float
    spoof_score: float
    spoof_type: str = 'unknown'
    computed_speed_kn: float = 0.0
    reported_speed_kn: float = 0.0
    computed_bearing: float = 0.0
    reported_cog: float = 0.0
    details: str = ''

    @property
    def dt(self) -> datetime:
        return datetime.fromtimestamp(self.epoch, tz=timezone.utc).replace(tzinfo=None)

    def __repr__(self) -> str:
        return (
            f'SpoofEvent(mmsi={self.mmsi}, type={self.spoof_type!r}, '
            f'at={self.dt.isoformat()}, score={self.spoof_score:.3f}, '
            f'computed={self.computed_speed_kn:.1f}kn '
            f'reported={self.reported_speed_kn:.1f}kn)'
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalise_ship_type(track: dict) -> str:
    raw = track.get('ship_type_txt', '') or track.get('ship_type', '') or ''
    raw = str(raw).lower()
    for key in MAX_SPEED_KNOTS:
        if key in raw:
            return key
    return 'default'


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    '''Haversine distance in metres between two (lat, lon) pairs.'''
    R = 6_371_088.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlam = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    '''True bearing (0–360°) from point 1 to point 2.'''
    lat1r, lat2r = np.radians(lat1), np.radians(lat2)
    dlam = np.radians(lon2 - lon1)
    x = np.sin(dlam) * np.cos(lat2r)
    y = np.cos(lat1r) * np.sin(lat2r) - np.sin(lat1r) * np.cos(lat2r) * np.cos(dlam)
    return (np.degrees(np.arctan2(x, y)) + 360) % 360


def _angular_diff_deg(a: float, b: float) -> float:
    '''Smallest absolute angular difference between two bearings (0–180°).'''
    diff = abs(a - b) % 360
    return min(diff, 360 - diff)


def _sigmoid(x: float, centre: float, width: float) -> float:
    '''Logistic sigmoid in [0,1] centred at *centre* with half-width *width*.'''
    return float(1.0 / (1.0 + np.exp(-((x - centre) / max(width, 1e-9)))))


# ---------------------------------------------------------------------------
# Per-ping indicator scoring
# ---------------------------------------------------------------------------

def _kinematic_score(
    computed_kn: float,
    max_kn: float,
) -> float:
    '''Score how impossible the implied speed is for this vessel type.

    0.0 → speed within envelope
    1.0 → speed far exceeds physical limit

    Uses a sigmoid ramp centred at *max_kn* with half-width 0.3 × max_kn
    so the score rises sharply once the threshold is breached.
    '''
    return _sigmoid(computed_kn, max_kn, max_kn * 0.3)


def _sog_delta_score(computed_kn: float, reported_kn: float) -> float:
    '''Score for SOG inconsistency between computed and reported values.

    Unavailable SOG values (> 102.2 knots per AIS spec) are ignored.
    '''
    if reported_kn > 102.0:   # AIS "not available" sentinel
        return 0.0
    delta = abs(computed_kn - reported_kn)
    return _sigmoid(delta, SOG_TOLERANCE_KN, SOG_TOLERANCE_KN * 0.4)


def _cog_delta_score(computed_bearing: float, reported_cog: float) -> float:
    '''Score for COG inconsistency between computed bearing and reported COG.

    Unavailable COG values (≥ 360° per AIS spec) are ignored.
    Only meaningful when the vessel is actually moving (computed_speed > 0.5 kn);
    a stationary vessel has undefined bearing.
    '''
    if reported_cog >= 360.0:  # AIS "not available" sentinel
        return 0.0
    diff = _angular_diff_deg(computed_bearing, reported_cog)
    return _sigmoid(diff, COG_TOLERANCE_DEG, COG_TOLERANCE_DEG * 0.4)


def _interval_score(dt_s: float, nominal_s: float) -> float:
    '''Score for suspiciously short transmission intervals.

    Sub-second intervals are almost certainly injected messages.
    Intervals below MIN_CREDIBLE_INTERVAL_S score highly regardless of type.
    '''
    if dt_s < MIN_CREDIBLE_INTERVAL_S:
        return 1.0
    # Also flag intervals that are dramatically shorter than nominal
    ratio = nominal_s / max(dt_s, 0.1)
    if ratio > 10:              # more than 10× faster than expected
        return _sigmoid(ratio, 10.0, 3.0)
    return 0.0


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

class SpoofingDetector:
    '''Detects AIS message spoofing using physics-based indicator scoring.

    No training is required.  All checks are based on the physical
    constraints of VHF radio propagation, maritime vessel performance,
    and the ITU-R M.1371 AIS standard.

    Four per-ping indicators are computed:

    ``kinematic``
        Implied speed between consecutive positions vs vessel-type envelope.
        A vessel that "teleports" will always score 1.0 here.

    ``sog_delta``
        Absolute difference between AIS-reported SOG and haversine-derived
        speed.  A spoofer injecting a false position without updating the
        kinematic fields leaves a detectable mismatch.

    ``cog_delta``
        Angular deviation between AIS-reported COG and the true bearing
        from the previous position.  Flagged when > 30° and vessel is
        moving.

    ``interval``
        Transmission interval shorter than MIN_CREDIBLE_INTERVAL_S (2 s)
        or >10× faster than the nominal rate for the vessel type, indicating
        software-injected messages.

    Composite score = weighted mean of all four indicators.

    Example::

        detector = SpoofingDetector()

        # Per-track annotation
        for track in detector.flag_spoofing(tracks, threshold=0.6):
            for event in track['spoof_events']:
                print(event)

        # Cross-track MMSI conflict detection
        for track in detector.check_mmsi_conflicts(tracks):
            conflicts = [e for e in track['spoof_events']
                         if e.spoof_type == 'mmsi_conflict']
    '''

    #: Weights for the four indicators in the composite score.
    WEIGHTS = {
        'kinematic': 0.40,
        'sog_delta': 0.25,
        'cog_delta': 0.20,
        'interval':  0.15,
    }

    def __init__(self) -> None:
        # Rolling per-MMSI last-seen record for MMSI conflict detection.
        # Maps mmsi → (epoch, lat, lon)
        self._mmsi_registry: Dict[int, Tuple[int, float, float]] = {}

    # ------------------------------------------------------------------
    # Per-track scoring
    # ------------------------------------------------------------------

    def score_track(self, track: dict) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        '''Compute per-ping indicator scores for a single track.

        Returns a tuple of five numpy arrays, each of shape ``(n_pings - 1,)``:
            (composite, kinematic, sog_delta, cog_delta, interval)

        All values are in [0, 1].  Returns empty arrays for single-ping tracks.
        '''
        time = track['time'].astype(np.float64)
        lat = track['lat'].astype(np.float64)
        lon = track['lon'].astype(np.float64)
        sog_rep = track.get('sog', np.zeros(len(time))).astype(np.float64)
        cog_rep = track.get('cog', np.full(len(time), 511.0)).astype(np.float64)
        n = len(time)

        empty = np.array([], dtype=np.float32)
        if n < 2:
            return empty, empty, empty, empty, empty

        stype = _normalise_ship_type(track)
        max_kn = MAX_SPEED_KNOTS[stype]
        nominal_s = NOMINAL_INTERVAL_S[stype]

        kin_scores = np.zeros(n - 1, dtype=np.float32)
        sog_scores = np.zeros(n - 1, dtype=np.float32)
        cog_scores = np.zeros(n - 1, dtype=np.float32)
        int_scores = np.zeros(n - 1, dtype=np.float32)

        for i in range(n - 1):
            dt_s = max(time[i + 1] - time[i], 0.001)
            dist_m = _haversine_m(lat[i], lon[i], lat[i + 1], lon[i + 1])
            comp_kn = (dist_m / dt_s) * 1.94384   # m/s → knots

            bearing = _bearing_deg(lat[i], lon[i], lat[i + 1], lon[i + 1])

            kin_scores[i] = _kinematic_score(comp_kn, max_kn)
            # SOG and COG checks are only meaningful when vessel is moving
            if comp_kn > 0.2:
                sog_scores[i] = _sog_delta_score(comp_kn, float(sog_rep[i + 1]))
                cog_scores[i] = _cog_delta_score(bearing, float(cog_rep[i + 1]))
            int_scores[i] = _interval_score(dt_s, nominal_s)

        w = self.WEIGHTS
        composite = (
            w['kinematic'] * kin_scores
            + w['sog_delta'] * sog_scores
            + w['cog_delta'] * cog_scores
            + w['interval'] * int_scores
        ).astype(np.float32)

        return composite, kin_scores, sog_scores, cog_scores, int_scores

    def annotate(self, track: dict, threshold: float = 0.5) -> dict:
        '''Annotate a single track dict with spoofing indicator scores.

        Adds the following keys:

        ``spoof_scores`` (np.ndarray)
            Composite per-ping spoofing score in [0, 1], shape ``(n-1,)``.

        ``spoof_kinematic``, ``spoof_sog``, ``spoof_cog``, ``spoof_interval``
            Individual indicator arrays, same shape.

        ``spoof_events`` (list[SpoofEvent])
            One entry per ping whose composite score ≥ *threshold*.

        All score arrays are registered in ``track['dynamic']``.

        args:
            track:     AISdb track dict.
            threshold: Composite score cutoff in [0, 1].

        returns:
            The annotated track dict.
        '''
        composite, kin, sog_d, cog_d, intv = self.score_track(track)

        track['spoof_scores'] = composite
        track['spoof_kinematic'] = kin
        track['spoof_sog'] = sog_d
        track['spoof_cog'] = cog_d
        track['spoof_interval'] = intv
        track['dynamic'] = set(track.get('dynamic', set())) | {
            'spoof_scores', 'spoof_kinematic', 'spoof_sog', 'spoof_cog', 'spoof_interval',
        }

        events: List[SpoofEvent] = []
        if len(composite) == 0:
            track['spoof_events'] = events
            return track

        time = track['time']
        lat = track['lat'].astype(np.float64)
        lon = track['lon'].astype(np.float64)
        sog_rep = track.get('sog', np.zeros(len(time))).astype(np.float64)
        cog_rep = track.get('cog', np.full(len(time), 511.0)).astype(np.float64)

        for i in np.where(composite >= threshold)[0]:
            dt_s = max(float(time[i + 1]) - float(time[i]), 0.001)
            dist_m = _haversine_m(lat[i], lon[i], lat[i + 1], lon[i + 1])
            comp_kn = (dist_m / dt_s) * 1.94384
            bearing = _bearing_deg(lat[i], lon[i], lat[i + 1], lon[i + 1])

            # Identify the dominant indicator for this ping
            indicators = {
                'kinematic': float(kin[i]),
                'sog_delta': float(sog_d[i]),
                'cog_delta': float(cog_d[i]),
                'interval':  float(intv[i]),
            }
            dominant = max(indicators, key=indicators.get)

            details_map = {
                'kinematic': f'implied speed {comp_kn:.1f} kn exceeds envelope',
                'sog_delta': f'SOG mismatch: computed={comp_kn:.1f} reported={sog_rep[i+1]:.1f} kn',
                'cog_delta': f'COG mismatch: bearing={bearing:.0f}° reported={cog_rep[i+1]:.0f}°',
                'interval':  f'interval {dt_s:.1f}s below credible minimum',
            }

            events.append(SpoofEvent(
                mmsi=int(track.get('mmsi', 0)),
                epoch=int(time[i + 1]),
                lat=float(lat[i + 1]),
                lon=float(lon[i + 1]),
                spoof_score=float(composite[i]),
                spoof_type=dominant,
                computed_speed_kn=comp_kn,
                reported_speed_kn=float(sog_rep[i + 1]),
                computed_bearing=bearing,
                reported_cog=float(cog_rep[i + 1]),
                details=details_map[dominant],
            ))

        track['spoof_events'] = events
        return track

    def flag_spoofing(
        self,
        tracks: Iterable[dict],
        threshold: float = 0.5,
    ) -> Generator[dict, None, None]:
        '''Generator that annotates each track with spoofing indicator scores.

        Drop-in pipeline filter::

            for track in detector.flag_spoofing(TrackGen(rowgen), threshold=0.6):
                for event in track['spoof_events']:
                    print(event)

        args:
            tracks:    AISdb track generator.
            threshold: Composite score cutoff in [0, 1].

        yields:
            Track dicts with ``spoof_scores`` and ``spoof_events`` added.
        '''
        for track in tracks:
            yield self.annotate(track, threshold=threshold)

    # ------------------------------------------------------------------
    # Cross-track MMSI conflict detection
    # ------------------------------------------------------------------

    def check_mmsi_conflicts(
        self,
        tracks: Iterable[dict],
        threshold: float = 0.5,
    ) -> Generator[dict, None, None]:
        '''Generator that annotates tracks AND checks for MMSI conflicts.

        Maintains a rolling registry of the most recent position seen for
        each MMSI.  When a new track for the same MMSI arrives and the
        implied transit speed from the last known position exceeds
        ``MMSI_CONFLICT_SPEED_KN``, a ``SpoofEvent`` of type
        ``'mmsi_conflict'`` is prepended to ``track['spoof_events']``.

        This catches the most common AIS spoofing attack: a vessel that
        broadcasts a false MMSI to impersonate another ship (or to hide
        its own identity), resulting in the same MMSI appearing
        simultaneously in two physically-separated locations.

        args:
            tracks:    AISdb track generator.
            threshold: Composite kinematic score cutoff for per-ping events.

        yields:
            Annotated track dicts, possibly with ``mmsi_conflict`` events.
        '''
        for track in self.flag_spoofing(tracks, threshold=threshold):
            mmsi = int(track.get('mmsi', 0))
            time = track['time']
            lat = track['lat'].astype(np.float64)
            lon = track['lon'].astype(np.float64)

            if mmsi and len(time) >= 1:
                first_epoch = int(time[0])
                first_lat = float(lat[0])
                first_lon = float(lon[0])

                if mmsi in self._mmsi_registry:
                    prev_epoch, prev_lat, prev_lon = self._mmsi_registry[mmsi]
                    dt_s = max(abs(first_epoch - prev_epoch), 1.0)
                    dist_m = _haversine_m(prev_lat, prev_lon, first_lat, first_lon)
                    implied_kn = (dist_m / dt_s) * 1.94384

                    if implied_kn > MMSI_CONFLICT_SPEED_KN:
                        conflict = SpoofEvent(
                            mmsi=mmsi,
                            epoch=first_epoch,
                            lat=first_lat,
                            lon=first_lon,
                            spoof_score=min(1.0, implied_kn / MMSI_CONFLICT_SPEED_KN),
                            spoof_type='mmsi_conflict',
                            computed_speed_kn=implied_kn,
                            details=(
                                f'MMSI {mmsi} implies {implied_kn:.0f} kn transit '
                                f'from last known position — likely duplicate MMSI '
                                f'or position spoofing'
                            ),
                        )
                        track['spoof_events'].insert(0, conflict)
                        logger.warning(
                            'MMSI conflict detected: %d at %.4f,%.4f '
                            '(implied %.0f kn from last position)',
                            mmsi, first_lat, first_lon, implied_kn,
                        )

                # Update registry with last position in this track
                self._mmsi_registry[mmsi] = (int(time[-1]), float(lat[-1]), float(lon[-1]))

            yield track

    def reset_mmsi_registry(self) -> None:
        '''Clear the MMSI conflict registry (call between unrelated batches).'''
        self._mmsi_registry.clear()

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        '''Save MMSI registry state to *path* (pickle).'''
        with open(path, 'wb') as fh:
            pickle.dump({'mmsi_registry': self._mmsi_registry}, fh)
        logger.info('Saved SpoofingDetector → %s', path)

    @classmethod
    def load(cls, path: str) -> 'SpoofingDetector':
        '''Load a previously saved SpoofingDetector registry from *path*.'''
        with open(path, 'rb') as fh:
            state = pickle.load(fh)
        obj = cls()
        obj._mmsi_registry = state['mmsi_registry']
        logger.info('Loaded SpoofingDetector ← %s', path)
        return obj

    def __repr__(self) -> str:
        return f'SpoofingDetector(mmsi_registry_size={len(self._mmsi_registry)})'


# ---------------------------------------------------------------------------
# Module-level pipeline function
# ---------------------------------------------------------------------------

def flag_spoofing(
    tracks: Iterable[dict],
    detector: Optional[SpoofingDetector] = None,
    threshold: float = 0.5,
) -> Generator[dict, None, None]:
    '''Annotate tracks with AIS spoofing indicator scores.

    Drop-in pipeline filter.  If no *detector* is supplied a new
    ``SpoofingDetector`` is created (no training or state required).

    Example::

        from aisdb.anomaly.spoof_detect import flag_spoofing

        for track in flag_spoofing(TrackGen(rowgen, decimate=True)):
            for event in track['spoof_events']:
                print(event)

    args:
        tracks:    AISdb track generator (TrackGen or any pipeline filter).
        detector:  :class:`SpoofingDetector` instance, or ``None``.
        threshold: Composite score cutoff in [0, 1].

    yields:
        Annotated track dicts.
    '''
    if detector is None:
        detector = SpoofingDetector()
    yield from detector.flag_spoofing(tracks, threshold=threshold)

'''LSTM Autoencoder for Vessel Trajectory Anomaly Detection

Detects anomalous vessel behaviour from AIS trajectory sequences using a
sequence-to-sequence LSTM autoencoder.  The model is trained on normal
trajectories; at inference time, tracks that cannot be reconstructed
accurately are flagged as anomalous.

Typical anomalies captured:
- **Loitering** — slow circular motion inconsistent with the vessel's type
- **Sudden course reversal** — sharp heading change at speed
- **Erratic speed profile** — alternating stop/sprint behaviour
- **Physically implausible manoeuvres** — exceeding vessel performance limits

Feature vector (per-gap, location-invariant):
  [log1p(dist_m), log1p(dt_s), sog, sin(cog), cos(cog), Δcog_norm]

where:
  - dist_m   — haversine distance to next ping (metres)
  - dt_s     — elapsed seconds to next ping
  - sog      — speed over ground (knots, from AIS message)
  - cog      — course over ground (degrees, decomposed to sin/cos)
  - Δcog_norm — normalised course change in [-1, 1]

Using deltas rather than absolute coordinates makes the model
vessel-position-agnostic and directly comparable across ocean regions.

Two operating modes:
  1. **Statistical baseline** (no training required) — Isolation Forest on
     the feature matrix; useful for quick evaluation.
  2. **LSTM Autoencoder** (trained) — sliding-window reconstruction; higher
     accuracy for normal/anomalous separation.

Pipeline usage::

    from aisdb.anomaly.lstm_anomaly import TrajectoryAnomalyDetector, flag_anomalies

    # Zero-config statistical mode
    for track in flag_anomalies(TrackGen(rowgen, decimate=True)):
        for event in track['anomaly_events']:
            print(event)

    # LSTM mode
    detector = TrajectoryAnomalyDetector(mode='lstm')
    detector.fit(training_tracks)
    detector.save('lstm_model.pt')

    detector = TrajectoryAnomalyDetector.load('lstm_model.pt')
    for track in flag_anomalies(live_tracks, detector, threshold=0.8):
        for event in track['anomaly_events']:
            print(event)
'''

import logging
import pickle
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Generator, Iterable, List, Literal, Optional, Tuple

import numpy as np
import torch
import torch.nn as nn
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Number of input features per timestep gap.
N_FEATURES = 6

#: Sliding window length (number of gap-steps per window).
DEFAULT_WINDOW = 32

#: Window stride during training and inference.
DEFAULT_STRIDE = 16

#: Minimum number of pings in a track to attempt anomaly scoring.
MIN_TRACK_LENGTH = 8

#: Anomaly type labels inferred from the dominant feature deviation.
_ANOMALY_TYPES = {
    'loitering': 'Vessel moving slowly in a small area',
    'course_reversal': 'Abrupt change in course at speed',
    'erratic_speed': 'Highly variable speed profile',
    'implausible': 'Physically implausible trajectory',
    'unknown': 'Anomalous trajectory (unclassified)',
}


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class AnomalyEvent:
    '''A detected trajectory anomaly segment.

    Attributes:
        mmsi:          Maritime Mobile Service Identity.
        start_epoch:   Unix epoch seconds of first ping in the anomalous segment.
        end_epoch:     Unix epoch seconds of last ping in the anomalous segment.
        start_lat:     Latitude at start of segment.
        start_lon:     Longitude at start of segment.
        end_lat:       Latitude at end of segment.
        end_lon:       Longitude at end of segment.
        anomaly_score: Normalised score in [0, 1] (1 = most anomalous).
        anomaly_type:  Inferred category string.
        n_pings:       Number of pings in the segment.
    '''
    mmsi: int
    start_epoch: int
    end_epoch: int
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    anomaly_score: float
    anomaly_type: str = 'unknown'
    n_pings: int = 0

    @property
    def duration_s(self) -> float:
        return float(self.end_epoch - self.start_epoch)

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
            f'AnomalyEvent(mmsi={self.mmsi}, type={self.anomaly_type!r}, '
            f'start={self.start_dt.isoformat()}, '
            f'duration={self.duration_hours:.2f}h, score={self.anomaly_score:.3f})'
        )


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

def _extract_features(track: dict) -> np.ndarray:
    '''Extract a location-invariant feature matrix from a track dict.

    Returns shape ``(n_pings - 1, N_FEATURES)`` or an empty array if the
    track is too short.  All features are finite-checked; rows containing
    NaN or Inf are replaced with zeros.

    Features (per gap between consecutive pings):
      0: log1p(haversine distance in metres)
      1: log1p(elapsed seconds)
      2: speed over ground (knots, clipped to [0, 50])
      3: sin(course over ground in radians)
      4: cos(course over ground in radians)
      5: normalised course change in [-1, 1]  (Δcog / 180)
    '''
    time = track['time'].astype(np.float64)
    lon = track['lon'].astype(np.float64)
    lat = track['lat'].astype(np.float64)
    n = len(time)

    if n < 2:
        return np.empty((0, N_FEATURES), dtype=np.float32)

    # --- haversine distances (metres) ---
    lat_r = np.radians(lat)
    lon_r = np.radians(lon)
    dlat = np.diff(lat_r)
    dlon = np.diff(lon_r)
    a = np.sin(dlat / 2) ** 2 + np.cos(lat_r[:-1]) * np.cos(lat_r[1:]) * np.sin(dlon / 2) ** 2
    dist_m = 2 * 6_371_088 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))

    # --- time deltas ---
    dt_s = np.maximum(np.diff(time), 1.0)

    # --- speed over ground (from AIS message, clipped) ---
    sog = np.clip(track.get('sog', np.zeros(n)).astype(np.float64)[:-1], 0.0, 50.0)

    # --- course over ground ---
    cog_raw = track.get('cog', np.zeros(n)).astype(np.float64)
    # AIS COG values > 360 indicate unavailable; replace with bearing-derived estimate
    unavailable = cog_raw > 360.0
    if unavailable.any():
        bearing = np.degrees(np.arctan2(dlon, dlat)) % 360
        # bearing has n-1 values; pad to n by repeating last
        bearing_full = np.append(bearing, bearing[-1] if len(bearing) else 0.0)
        cog_raw = np.where(unavailable, bearing_full, cog_raw)
    cog_rad = np.radians(cog_raw[:-1])

    # --- course change (normalised) ---
    dcog = np.diff(cog_raw)
    # wrap to [-180, 180]
    dcog = ((dcog + 180) % 360) - 180
    dcog_norm = dcog / 180.0  # in [-1, 1]

    feats = np.column_stack([
        np.log1p(dist_m),           # 0
        np.log1p(dt_s),             # 1
        sog,                         # 2
        np.sin(cog_rad),             # 3
        np.cos(cog_rad),             # 4
        dcog_norm,                   # 5
    ]).astype(np.float32)

    # Replace non-finite values with zeros
    feats = np.where(np.isfinite(feats), feats, 0.0)
    return feats


def _sliding_windows(
    feats: np.ndarray,
    window: int,
    stride: int,
) -> Tuple[np.ndarray, np.ndarray]:
    '''Slice a feature matrix into overlapping fixed-length windows.

    Returns:
        windows: shape (n_windows, window, N_FEATURES)
        indices: shape (n_windows,) — start index of each window in feats
    '''
    n = len(feats)
    if n < window:
        # Pad with zeros if shorter than window
        pad = np.zeros((window - n, N_FEATURES), dtype=np.float32)
        feats = np.vstack([feats, pad])
        return feats[np.newaxis], np.array([0])

    starts = list(range(0, n - window + 1, stride))
    if not starts or starts[-1] + window < n:
        starts.append(max(0, n - window))

    windows = np.stack([feats[s:s + window] for s in starts])
    return windows, np.array(starts)


def _window_scores_to_ping_scores(
    window_scores: np.ndarray,
    window_starts: np.ndarray,
    window_size: int,
    n_gaps: int,
) -> np.ndarray:
    '''Map per-window anomaly scores back to per-gap scores by averaging.

    Each gap index accumulates scores from all windows that cover it.
    Returns shape (n_gaps,).
    '''
    accum = np.zeros(n_gaps, dtype=np.float64)
    count = np.zeros(n_gaps, dtype=np.float64)
    for score, start in zip(window_scores, window_starts):
        end = min(start + window_size, n_gaps)
        accum[start:end] += score
        count[start:end] += 1
    count = np.maximum(count, 1)
    return (accum / count).astype(np.float32)


def _infer_anomaly_type(feats: np.ndarray) -> str:
    '''Infer the most likely anomaly category from feature deviations.'''
    if feats.shape[0] == 0:
        return 'unknown'
    dist = feats[:, 0]   # log dist
    sog = feats[:, 2]
    dcog = feats[:, 5]

    low_dist = np.median(dist) < np.log1p(50)       # median < 50 m/gap
    low_sog = np.median(sog) < 0.5                   # knots
    high_dcog = np.mean(np.abs(dcog)) > 0.3          # >54 deg mean change
    high_sog_var = np.std(sog) > 3.0                 # knots std

    if low_dist and low_sog:
        return 'loitering'
    if high_dcog and not low_sog:
        return 'course_reversal'
    if high_sog_var:
        return 'erratic_speed'
    if np.any(sog > 45):
        return 'implausible'
    return 'unknown'


# ---------------------------------------------------------------------------
# LSTM Autoencoder (PyTorch)
# ---------------------------------------------------------------------------

class _LSTMEncoder(nn.Module):
    def __init__(self, input_size: int, hidden_size: int, n_layers: int, latent_size: int):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, n_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, latent_size)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq, features)
        _, (h, _) = self.lstm(x)
        return self.fc(h[-1])  # (batch, latent_size)


class _LSTMDecoder(nn.Module):
    def __init__(self, latent_size: int, hidden_size: int, n_layers: int, output_size: int, seq_len: int):
        super().__init__()
        self.seq_len = seq_len
        self.fc = nn.Linear(latent_size, hidden_size)
        self.lstm = nn.LSTM(hidden_size, hidden_size, n_layers, batch_first=True)
        self.out = nn.Linear(hidden_size, output_size)

    def forward(self, z: torch.Tensor) -> torch.Tensor:
        # z: (batch, latent_size) → repeat across time → reconstruct
        h = self.fc(z).unsqueeze(1).repeat(1, self.seq_len, 1)
        out, _ = self.lstm(h)
        return self.out(out)  # (batch, seq, output_size)


class _LSTMAutoencoder(nn.Module):
    '''Sequence-to-sequence LSTM autoencoder for trajectory reconstruction.'''

    def __init__(
        self,
        input_size: int = N_FEATURES,
        hidden_size: int = 64,
        latent_size: int = 16,
        n_layers: int = 2,
        seq_len: int = DEFAULT_WINDOW,
    ):
        super().__init__()
        self.encoder = _LSTMEncoder(input_size, hidden_size, n_layers, latent_size)
        self.decoder = _LSTMDecoder(latent_size, hidden_size, n_layers, input_size, seq_len)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        z = self.encoder(x)
        return self.decoder(z)

    def reconstruction_error(self, x: torch.Tensor) -> torch.Tensor:
        '''Return mean squared error per sample in the batch.'''
        x_hat = self.forward(x)
        return ((x - x_hat) ** 2).mean(dim=(1, 2))   # (batch,)


# ---------------------------------------------------------------------------
# Core detector
# ---------------------------------------------------------------------------

class TrajectoryAnomalyDetector:
    '''Detects anomalous AIS vessel trajectories using an LSTM autoencoder
    or Isolation Forest baseline.

    Two modes:

    ``'statistical'`` (default, no training required)
        Fits an Isolation Forest on extracted feature vectors.
        Works immediately without any track data.

    ``'lstm'``
        Trains a sequence-to-sequence LSTM autoencoder on sliding windows
        of normal trajectories.  Anomaly score = reconstruction error,
        normalised to [0, 1] against training-set statistics.

    Example — statistical mode::

        detector = TrajectoryAnomalyDetector()          # mode='statistical'
        for track in detector.flag_anomalies(tracks):
            for ev in track['anomaly_events']:
                print(ev)

    Example — LSTM mode::

        detector = TrajectoryAnomalyDetector(mode='lstm')
        detector.fit(training_tracks)
        detector.save('model.pt')
        ...
        detector = TrajectoryAnomalyDetector.load('model.pt')
        for track in detector.flag_anomalies(live_tracks, threshold=0.75):
            ...
    '''

    def __init__(
        self,
        mode: Literal['statistical', 'lstm'] = 'statistical',
        window_size: int = DEFAULT_WINDOW,
        stride: int = DEFAULT_STRIDE,
        hidden_size: int = 64,
        latent_size: int = 16,
        n_layers: int = 2,
    ) -> None:
        self.mode = mode
        self.window_size = window_size
        self.stride = stride

        # Shared
        self._scaler = StandardScaler()
        self._fitted = False
        self._threshold_95: float = 0.5   # 95th-pct training error (lstm) or score (iso)

        # Statistical mode
        self._iforest: Optional[IsolationForest] = None

        # LSTM mode
        self._lstm: Optional[_LSTMAutoencoder] = None
        self._lstm_config: Dict = dict(
            hidden_size=hidden_size,
            latent_size=latent_size,
            n_layers=n_layers,
            seq_len=window_size,
        )

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def fit(
        self,
        tracks: Iterable[dict],
        epochs: int = 20,
        batch_size: int = 64,
        lr: float = 1e-3,
        contamination: float = 0.05,
    ) -> 'TrajectoryAnomalyDetector':
        '''Fit the detector on a collection of (assumed-normal) tracks.

        For ``mode='statistical'`` trains an Isolation Forest.
        For ``mode='lstm'`` trains the LSTM autoencoder.

        args:
            tracks:        Iterable of AISdb track dicts.
            epochs:        (LSTM only) Training epochs.
            batch_size:    (LSTM only) Mini-batch size.
            lr:            (LSTM only) Adam learning rate.
            contamination: (statistical only) Expected fraction of outliers
                           in the training data (passed to IsolationForest).

        returns:
            self — allows method chaining.
        '''
        all_feats: List[np.ndarray] = []
        all_windows: List[np.ndarray] = []

        for track in tracks:
            feats = _extract_features(track)
            if feats.shape[0] < MIN_TRACK_LENGTH - 1:
                continue
            all_feats.append(feats)
            if self.mode == 'lstm':
                wins, _ = _sliding_windows(feats, self.window_size, self.stride)
                all_windows.append(wins)

        if not all_feats:
            logger.warning('No usable tracks for fitting — detector will use defaults.')
            return self

        # Fit scaler on raw per-gap features
        flat = np.vstack(all_feats)
        self._scaler.fit(flat)

        if self.mode == 'statistical':
            self._fit_statistical(flat, contamination)
        else:
            self._fit_lstm(all_windows, epochs, batch_size, lr)

        self._fitted = True
        return self

    def _fit_statistical(self, flat_feats: np.ndarray, contamination: float) -> None:
        scaled = self._scaler.transform(flat_feats)
        self._iforest = IsolationForest(
            n_estimators=200,
            contamination=contamination,
            random_state=42,
            n_jobs=-1,
        )
        self._iforest.fit(scaled)
        # Calibrate threshold: raw scores from IsolationForest are negative
        # (more negative = more anomalous); convert to [0,1] anomaly probability
        raw = -self._iforest.score_samples(scaled)   # positive = anomalous
        self._threshold_95 = float(np.percentile(raw, 95))
        logger.info('Fitted IsolationForest on %d gap-features', flat_feats.shape[0])

    def _fit_lstm(
        self,
        all_windows: List[np.ndarray],
        epochs: int,
        batch_size: int,
        lr: float,
    ) -> None:
        if not all_windows:
            logger.warning('No windows to train LSTM on.')
            return

        # Stack and scale windows
        raw = np.vstack(all_windows).astype(np.float32)          # (N, T, F)
        N, T, F = raw.shape
        flat = raw.reshape(-1, F)
        flat_scaled = self._scaler.transform(flat).astype(np.float32)
        data = torch.tensor(flat_scaled.reshape(N, T, F))

        self._lstm = _LSTMAutoencoder(**self._lstm_config)
        optimiser = torch.optim.Adam(self._lstm.parameters(), lr=lr)
        criterion = nn.MSELoss()

        self._lstm.train()
        for epoch in range(epochs):
            perm = torch.randperm(N)
            epoch_loss = 0.0
            for i in range(0, N, batch_size):
                batch = data[perm[i:i + batch_size]]
                optimiser.zero_grad()
                recon = self._lstm(batch)
                loss = criterion(recon, batch)
                loss.backward()
                optimiser.step()
                epoch_loss += loss.item() * len(batch)
            if (epoch + 1) % 5 == 0 or epoch == 0:
                logger.info('LSTM epoch %d/%d — loss %.4f', epoch + 1, epochs, epoch_loss / N)

        # Calibrate threshold on training reconstruction errors
        self._lstm.eval()
        with torch.no_grad():
            errors = self._lstm.reconstruction_error(data).numpy()
        self._threshold_95 = float(np.percentile(errors, 95))
        logger.info('LSTM fitted. 95th-pct training error: %.4f', self._threshold_95)

    # ------------------------------------------------------------------
    # Inference helpers
    # ------------------------------------------------------------------

    def _scale(self, feats: np.ndarray) -> np.ndarray:
        '''Scale features, fitting the scaler on-the-fly if not yet fitted.'''
        from sklearn.exceptions import NotFittedError
        try:
            return self._scaler.transform(feats)
        except NotFittedError:
            # No training data seen yet — fit on the current track as a proxy
            self._scaler.fit(feats)
            return self._scaler.transform(feats)

    def _score_track_statistical(self, feats: np.ndarray) -> np.ndarray:
        '''Return per-gap anomaly scores in [0, 1] using Isolation Forest.'''
        scaled = self._scale(feats)
        if self._iforest is not None:
            raw = -self._iforest.score_samples(scaled)
        else:
            # Unfitted: use z-score magnitude as proxy
            raw = np.linalg.norm(scaled, axis=1)
        # Normalise to [0, 1] via sigmoid centred at 95th pct
        denom = max(self._threshold_95, 1e-6)
        return (1.0 / (1.0 + np.exp(-(raw - denom) / denom))).astype(np.float32)

    def _score_track_lstm(self, feats: np.ndarray) -> np.ndarray:
        '''Return per-gap anomaly scores in [0, 1] using LSTM reconstruction.'''
        if self._lstm is None:
            return self._score_track_statistical(feats)

        scaled = self._scale(feats).astype(np.float32)
        windows, starts = _sliding_windows(scaled, self.window_size, self.stride)
        tensor = torch.tensor(windows)

        self._lstm.eval()
        with torch.no_grad():
            errors = self._lstm.reconstruction_error(tensor).numpy()

        ping_errors = _window_scores_to_ping_scores(errors, starts, self.window_size, len(feats))
        denom = max(self._threshold_95, 1e-6)
        return (1.0 / (1.0 + np.exp(-(ping_errors - denom) / denom))).astype(np.float32)

    def _score_track(self, feats: np.ndarray) -> np.ndarray:
        if self.mode == 'lstm':
            return self._score_track_lstm(feats)
        return self._score_track_statistical(feats)

    # ------------------------------------------------------------------
    # Annotation
    # ------------------------------------------------------------------

    def annotate(self, track: dict, threshold: float = 0.5) -> dict:
        '''Annotate a single track dict with trajectory anomaly scores.

        Adds the following keys:

        ``anomaly_scores`` (np.ndarray)
            Per-gap anomaly score in [0, 1], shape ``(n_pings - 1,)``.

        ``anomaly_events`` (list[AnomalyEvent])
            Contiguous runs of gaps whose score >= *threshold*.

        ``anomaly_scores`` is registered in ``track['dynamic']``.

        args:
            track:     AISdb track dict.
            threshold: Anomaly score cutoff in [0, 1].

        returns:
            The annotated track dict.
        '''
        feats = _extract_features(track)

        if feats.shape[0] == 0:
            track['anomaly_scores'] = np.array([], dtype=np.float32)
            track['anomaly_events'] = []
            return track

        scores = self._score_track(feats)
        track['anomaly_scores'] = scores
        track['dynamic'] = set(track.get('dynamic', set())) | {'anomaly_scores'}

        # Build contiguous anomaly events
        events: List[AnomalyEvent] = []
        time = track['time']
        lat = track['lat']
        lon = track['lon']
        above = scores >= threshold

        i = 0
        while i < len(above):
            if not above[i]:
                i += 1
                continue
            # Start of a contiguous anomalous run
            j = i
            while j < len(above) and above[j]:
                j += 1
            # Segment: gap indices [i, j) → ping indices [i, j+1)
            seg_start_ping = i
            seg_end_ping = min(j, len(time) - 1)
            seg_feats = feats[i:j]
            events.append(AnomalyEvent(
                mmsi=int(track.get('mmsi', 0)),
                start_epoch=int(time[seg_start_ping]),
                end_epoch=int(time[seg_end_ping]),
                start_lat=float(lat[seg_start_ping]),
                start_lon=float(lon[seg_start_ping]),
                end_lat=float(lat[seg_end_ping]),
                end_lon=float(lon[seg_end_ping]),
                anomaly_score=float(scores[i:j].max()),
                anomaly_type=_infer_anomaly_type(seg_feats),
                n_pings=seg_end_ping - seg_start_ping + 1,
            ))
            i = j

        track['anomaly_events'] = events
        return track

    def flag_anomalies(
        self,
        tracks: Iterable[dict],
        threshold: float = 0.5,
    ) -> Generator[dict, None, None]:
        '''Generator that annotates each track with trajectory anomaly info.

        Drop-in pipeline filter::

            tracks = TrackGen(rowgen, decimate=True)
            for track in detector.flag_anomalies(tracks, threshold=0.7):
                for event in track['anomaly_events']:
                    print(event)

        args:
            tracks:    AISdb track generator.
            threshold: Anomaly score cutoff in [0, 1].

        yields:
            Track dicts with ``anomaly_scores`` and ``anomaly_events`` added.
        '''
        for track in tracks:
            yield self.annotate(track, threshold=threshold)

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        '''Save the fitted detector to *path*.

        For LSTM mode the PyTorch model state dict is embedded in the
        pickle alongside the scaler and metadata.
        '''
        state = {
            'mode': self.mode,
            'window_size': self.window_size,
            'stride': self.stride,
            'lstm_config': self._lstm_config,
            'scaler': self._scaler,
            'threshold_95': self._threshold_95,
            'fitted': self._fitted,
            'iforest': self._iforest,
            'lstm_state': self._lstm.state_dict() if self._lstm is not None else None,
        }
        with open(path, 'wb') as fh:
            pickle.dump(state, fh)
        logger.info('Saved TrajectoryAnomalyDetector → %s', path)

    @classmethod
    def load(cls, path: str) -> 'TrajectoryAnomalyDetector':
        '''Load a previously saved TrajectoryAnomalyDetector from *path*.'''
        with open(path, 'rb') as fh:
            state = pickle.load(fh)
        cfg = state['lstm_config']
        obj = cls(
            mode=state['mode'],
            window_size=state['window_size'],
            stride=state['stride'],
            hidden_size=cfg['hidden_size'],
            latent_size=cfg['latent_size'],
            n_layers=cfg['n_layers'],
        )
        obj._scaler = state['scaler']
        obj._threshold_95 = state['threshold_95']
        obj._fitted = state['fitted']
        obj._iforest = state['iforest']
        if state['lstm_state'] is not None:
            obj._lstm = _LSTMAutoencoder(**state['lstm_config'])
            obj._lstm.load_state_dict(state['lstm_state'])
            obj._lstm.eval()
        logger.info('Loaded TrajectoryAnomalyDetector ← %s', path)
        return obj

    def __repr__(self) -> str:
        status = 'fitted' if self._fitted else 'unfitted'
        return f'TrajectoryAnomalyDetector(mode={self.mode!r}, {status})'


# ---------------------------------------------------------------------------
# Module-level pipeline function
# ---------------------------------------------------------------------------

def flag_anomalies(
    tracks: Iterable[dict],
    detector: Optional[TrajectoryAnomalyDetector] = None,
    threshold: float = 0.5,
) -> Generator[dict, None, None]:
    '''Annotate tracks with trajectory anomaly events.

    Convenience wrapper for TrackGen pipelines.  If no *detector* is given
    a default statistical (Isolation Forest) detector is created.

    Example::

        from aisdb.anomaly.lstm_anomaly import flag_anomalies

        for track in flag_anomalies(TrackGen(rowgen, decimate=True)):
            for event in track['anomaly_events']:
                print(event)

    args:
        tracks:    AISdb track generator (TrackGen or any filter).
        detector:  Fitted :class:`TrajectoryAnomalyDetector`, or ``None``
                   to use default statistical mode.
        threshold: Anomaly score cutoff in [0, 1].

    yields:
        Annotated track dicts.
    '''
    if detector is None:
        detector = TrajectoryAnomalyDetector(mode='statistical')
    yield from detector.flag_anomalies(tracks, threshold=threshold)

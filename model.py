from __future__ import annotations

import json
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Tuple

import numpy as np
from PIL import Image


ARTIFACT_PATH = Path(__file__).with_name("model_artifact.json")

FEATURE_NAMES = ("mean_luma", "std_luma", "lap_var", "edge_density")


@dataclass(frozen=True)
class ModelArtifact:
    version: str
    feature_names: Tuple[str, ...]
    x_mean: np.ndarray  # (d,)
    x_std: np.ndarray  # (d,)
    w: np.ndarray  # (d,)
    b: float

    def predict(self, x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=np.float32)
        z = (x - self.x_mean) / self.x_std
        y = z @ self.w + self.b
        return np.clip(y, 0.0, 10.0)


_CACHED: ModelArtifact | None = None


def _to_gray_np(image_bytes: bytes, max_size: int = 512) -> np.ndarray:
    """Decode bytes -> grayscale float32 array in [0, 1]."""
    with Image.open(BytesIO(image_bytes)) as im:
        im = im.convert("RGB")
        w, h = im.size
        m = max(w, h)
        if m > max_size:
            scale = max_size / float(m)
            im = im.resize((max(1, int(w * scale)), max(1, int(h * scale))), Image.Resampling.BILINEAR)
        gray = im.convert("L")
        arr = np.asarray(gray, dtype=np.float32) / 255.0
    return arr


def _laplacian_variance(gray: np.ndarray) -> float:
    # 2D 4-neighborhood Laplacian kernel:
    # [ 0  1  0
    #   1 -4  1
    #   0  1  0 ]
    g = np.asarray(gray, dtype=np.float32)
    gp = np.pad(g, 1, mode="edge")
    lap = (
        gp[0:-2, 1:-1]
        + gp[2:, 1:-1]
        + gp[1:-1, 0:-2]
        + gp[1:-1, 2:]
        - 4.0 * gp[1:-1, 1:-1]
    )
    return float(np.var(lap, dtype=np.float32))


def _edge_density(gray: np.ndarray) -> float:
    g = np.asarray(gray, dtype=np.float32)
    # Simple gradient magnitude using forward differences
    gx = np.zeros_like(g)
    gy = np.zeros_like(g)
    gx[:, 1:] = np.abs(g[:, 1:] - g[:, :-1])
    gy[1:, :] = np.abs(g[1:, :] - g[:-1, :])
    mag = gx + gy
    # Threshold tuned for [0,1] grayscale
    return float(np.mean(mag > 0.10))


def extract_features(image_bytes: bytes) -> Dict[str, float]:
    gray = _to_gray_np(image_bytes)
    mean_luma = float(np.mean(gray))
    std_luma = float(np.std(gray))
    lap_var = _laplacian_variance(gray)
    edge_density = _edge_density(gray)
    return {
        "mean_luma": mean_luma,
        "std_luma": std_luma,
        "lap_var": lap_var,
        "edge_density": edge_density,
    }


def _ridge_fit(X: np.ndarray, y: np.ndarray, alpha: float = 1.0) -> Tuple[np.ndarray, float]:
    """Fit ridge regression on standardized X (no intercept column)."""
    X = np.asarray(X, dtype=np.float32)
    y = np.asarray(y, dtype=np.float32)
    d = X.shape[1]
    A = X.T @ X + alpha * np.eye(d, dtype=np.float32)
    w = np.linalg.solve(A, X.T @ y)
    b = float(np.mean(y - X @ w))
    return w.astype(np.float32), b


def _build_fake_dataset(n: int, seed: int) -> Tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    # Feature ranges roughly matching extract_features():
    mean_luma = rng.uniform(0.05, 0.95, size=n)
    std_luma = rng.uniform(0.01, 0.40, size=n)
    lap_var = rng.lognormal(mean=-5.0, sigma=1.0, size=n)  # small positive values
    lap_var = np.clip(lap_var, 0.0, 0.25)
    edge_density = rng.uniform(0.00, 0.50, size=n)

    X = np.stack([mean_luma, std_luma, lap_var, edge_density], axis=1).astype(np.float32)

    # "Fake truth": sharper + some contrast + some edges, penalize extreme brightness.
    brightness_penalty = np.abs(mean_luma - 0.5)
    y = (
        6.0
        + 10.0 * (1.6 * lap_var + 0.8 * edge_density + 0.4 * std_luma - 0.9 * brightness_penalty)
        + rng.normal(0.0, 0.75, size=n)
    )
    y = np.clip(y, 0.0, 10.0).astype(np.float32)
    return X, y


def train_and_save_artifact(path: Path = ARTIFACT_PATH, n: int = 4000, seed: int = 7) -> ModelArtifact:
    X, y = _build_fake_dataset(n=n, seed=seed)
    x_mean = X.mean(axis=0)
    x_std = X.std(axis=0)
    x_std = np.where(x_std == 0, 1.0, x_std).astype(np.float32)
    Z = (X - x_mean) / x_std
    w, b = _ridge_fit(Z, y, alpha=2.0)

    artifact = ModelArtifact(
        version="fake-ridge-v1",
        feature_names=tuple(FEATURE_NAMES),
        x_mean=x_mean.astype(np.float32),
        x_std=x_std,
        w=w,
        b=b,
    )

    payload: Dict[str, Any] = {
        "version": artifact.version,
        "feature_names": list(artifact.feature_names),
        "x_mean": artifact.x_mean.tolist(),
        "x_std": artifact.x_std.tolist(),
        "w": artifact.w.tolist(),
        "b": artifact.b,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return artifact


def load_artifact(path: Path = ARTIFACT_PATH) -> ModelArtifact:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return ModelArtifact(
        version=str(payload["version"]),
        feature_names=tuple(payload["feature_names"]),
        x_mean=np.asarray(payload["x_mean"], dtype=np.float32),
        x_std=np.asarray(payload["x_std"], dtype=np.float32),
        w=np.asarray(payload["w"], dtype=np.float32),
        b=float(payload["b"]),
    )


def get_model() -> ModelArtifact:
    global _CACHED
    if _CACHED is not None:
        return _CACHED
    if not ARTIFACT_PATH.exists():
        _CACHED = train_and_save_artifact()
    else:
        _CACHED = load_artifact()
    return _CACHED


def predict_grade(image_bytes: bytes) -> Tuple[float, Dict[str, float]]:
    feats = extract_features(image_bytes)
    x = np.array([feats[n] for n in FEATURE_NAMES], dtype=np.float32)
    model = get_model()
    grade = float(model.predict(x))
    # Keep response compact + stable for UI
    feats_out = {k: float(v) for k, v in feats.items()}
    return grade, feats_out

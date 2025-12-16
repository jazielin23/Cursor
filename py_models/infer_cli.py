#!/usr/bin/env python3
"""Optional advanced model inference for the Shiny app.

Tasks:
  - imagenet_topk: EfficientNetV2 / ViT top-K labels (torchvision)
  - clip_quality: CLIP zero-shot quality score (open_clip)

This script is intentionally optional: if deps aren't installed, it returns a JSON error.

Example:
  python py_models/infer_cli.py imagenet_topk --arch efficientnet_v2_s --image path.jpg
  python py_models/infer_cli.py clip_quality --image path.jpg --pos "a high quality photo" --neg "a low quality blurry photo"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _err(message: str, hint: str | None = None, code: int = 1):
    payload = {
        "ok": False,
        "error": message,
        # Critical for debugging on Windows: confirm which Python is running.
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
    }
    if hint:
        payload["hint"] = hint
    print(json.dumps(payload))
    raise SystemExit(code)


def _load_image_pil(path: str):
    try:
        from PIL import Image
    except Exception as e:
        _err("Missing Python package: pillow", "pip install pillow")

    p = Path(path)
    if not p.exists():
        _err(f"Image not found: {path}")

    try:
        im = Image.open(p).convert("RGB")
    except Exception as e:
        _err("Failed to read image", str(e))
    return im


def imagenet_topk(arch: str, image_path: str, k: int):
    try:
        import torch
        from torchvision import models
        from torchvision.transforms import v2 as T
    except Exception as e:
        _err(
            "Missing Python packages: torch/torchvision",
            "Install PyTorch + torchvision for your Python. See: https://pytorch.org/get-started/locally/",
        )

    im = _load_image_pil(image_path)

    # Pick model + weights + label categories from torchvision metadata
    if arch == "efficientnet_v2_s":
        weights = models.EfficientNet_V2_S_Weights.DEFAULT
        model = models.efficientnet_v2_s(weights=weights)
    elif arch == "vit_b_16":
        weights = models.ViT_B_16_Weights.DEFAULT
        model = models.vit_b_16(weights=weights)
    elif arch == "vit_l_16":
        weights = models.ViT_L_16_Weights.DEFAULT
        model = models.vit_l_16(weights=weights)
    else:
        _err(f"Unsupported arch: {arch}", "Use efficientnet_v2_s, vit_b_16, vit_l_16")

    categories = weights.meta.get("categories")
    if not categories:
        _err("Could not load ImageNet labels from weights metadata")

    model.eval()

    preprocess = weights.transforms() if hasattr(weights, "transforms") else T.Compose(
        [
            T.Resize(256),
            T.CenterCrop(224),
            T.ToImage(),
            T.ToDtype(torch.float32, scale=True),
        ]
    )

    x = preprocess(im).unsqueeze(0)

    with torch.inference_mode():
        logits = model(x)
        probs = torch.softmax(logits, dim=1)[0]

    k = max(1, min(int(k), 20))
    topk = torch.topk(probs, k=k)

    out = []
    for idx, p in zip(topk.indices.tolist(), topk.values.tolist()):
        out.append({"label": categories[idx], "prob": float(p)})

    print(json.dumps({"ok": True, "task": "imagenet_topk", "arch": arch, "topk": out}))


def clip_quality(image_path: str, pos: str, neg: str):
    # CLIP is best-effort here. We use open_clip if installed.
    try:
        import torch
        import open_clip
    except Exception:
        _err(
            "Missing Python packages for CLIP: torch + open_clip_torch",
            "pip install open_clip_torch (and install torch first from pytorch.org)",
        )

    im = _load_image_pil(image_path)

    model, _, preprocess = open_clip.create_model_and_transforms("ViT-B-32", pretrained="laion2b_s34b_b79k")
    tokenizer = open_clip.get_tokenizer("ViT-B-32")
    model.eval()

    image = preprocess(im).unsqueeze(0)
    text = tokenizer([pos, neg])

    with torch.inference_mode():
        image_features = model.encode_image(image)
        text_features = model.encode_text(text)
        image_features = image_features / image_features.norm(dim=-1, keepdim=True)
        text_features = text_features / text_features.norm(dim=-1, keepdim=True)
        logits = (image_features @ text_features.T).squeeze(0)
        # logits[0]=pos similarity, logits[1]=neg similarity
        diff = (logits[0] - logits[1]).item()

    # Map diff to 0-10 with a smooth squashing; this is a heuristic score.
    import math

    score01 = 1.0 / (1.0 + math.exp(-diff))
    score10 = 10.0 * score01

    print(json.dumps({"ok": True, "task": "clip_quality", "pos": pos, "neg": neg, "diff": float(diff), "score": float(score10)}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("task", choices=["imagenet_topk", "clip_quality"])
    ap.add_argument("--image", required=True)

    ap.add_argument("--arch", default="efficientnet_v2_s")
    ap.add_argument("--k", type=int, default=5)

    ap.add_argument("--pos", default="a high quality, sharp photo")
    ap.add_argument("--neg", default="a low quality, blurry, noisy photo")

    args = ap.parse_args()

    if args.task == "imagenet_topk":
        imagenet_topk(args.arch, args.image, args.k)
    else:
        clip_quality(args.image, args.pos, args.neg)


if __name__ == "__main__":
    main()

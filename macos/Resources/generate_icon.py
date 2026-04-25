"""Generate a placeholder app icon — flat-colour rounded square with a 'YT' monogram.

Replace AppIcon.png with the final art when ready.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


HERE = Path(__file__).resolve().parent
WEB = HERE.parent.parent / "src" / "yt_scheduler" / "static" / "img"


def _load_font(size: int) -> ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/SFNS.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            try:
                return ImageFont.truetype(path, size=size)
            except OSError:
                continue
    return ImageFont.load_default()


def render(size: int) -> Image.Image:
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    radius = size // 5
    bg = (15, 15, 15, 255)
    accent = (62, 166, 255, 255)
    draw.rounded_rectangle((0, 0, size, size), radius=radius, fill=bg)

    # Inner accent stripe — subtle YouTube-ish red gradient stand-in.
    stripe_h = max(1, size // 24)
    stripe_y = int(size * 0.74)
    draw.rectangle(
        (size // 6, stripe_y, size - size // 6, stripe_y + stripe_h),
        fill=accent,
    )

    text = "YT"
    font = _load_font(size // 2)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    draw.text(
        ((size - tw) / 2 - bbox[0], (size - th) / 2 - bbox[1] - size // 18),
        text,
        font=font,
        fill=(241, 241, 241, 255),
    )
    return img


def main() -> None:
    HERE.mkdir(parents=True, exist_ok=True)
    WEB.mkdir(parents=True, exist_ok=True)

    icon_1024 = render(1024)
    icon_1024.save(HERE / "AppIcon.png")

    icon_512 = icon_1024.resize((512, 512), Image.LANCZOS)
    icon_512.save(WEB / "icon.png")

    icon_64 = icon_1024.resize((64, 64), Image.LANCZOS)
    icon_64.save(WEB / "favicon.png")

    print("Wrote", HERE / "AppIcon.png")
    print("Wrote", WEB / "icon.png")
    print("Wrote", WEB / "favicon.png")


if __name__ == "__main__":
    main()

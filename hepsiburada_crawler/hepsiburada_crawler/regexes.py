# url_classifier.py
import re

# Order matters: keep the most-specific patterns first.
PATTERNS = [
    # Product pages: "...-p-<alphanumeric>" (e.g. .../kulaklik-p-HBV00000ABCD)
    ("product", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/.+-p-[a-z0-9]+(?:[/?].*)?$",
        re.IGNORECASE,
    )),

    # Category pages: "...-c-<digits>" (e.g. .../kulakliklar-c-12345)
    ("category", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/.+-c-\d+(?:/|$|\?.*)",
        re.IGNORECASE,
    )),

    # Campaign/Promo hubs
    ("campaign", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/kampanyalar(?:/|$|\?.*)",
        re.IGNORECASE,
    )),

    # Seller storefronts
    ("seller_store", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/magaza/[^/?#]+(?:[/?].*)?$",
        re.IGNORECASE,
    )),

    # Brand landing pages
    ("brand", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/marka/[^/?#]+(?:[/?].*)?$",
        re.IGNORECASE,
    )),

    # Help / FAQ
    ("help", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/yardim(?:/|$|\?.*)",
        re.IGNORECASE,
    )),

    # Policy / legal / logistics info (broaden/adjust as needed)
    ("policy", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/"
        r"(?:gizlilik|kvkk|kisisel-veri|sozlesme|iade|kargo|teslimat)[^?#]*(?:[?#].*)?$",
        re.IGNORECASE,
    )),

    # Search pages
    ("search", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/(?:ara|search)(?:/|$|\?.*)",
        re.IGNORECASE,
    )),

    # Auth / account
    ("account", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/(?:uyelik|giris|hesabim|sifre)[^?#]*(?:[?#].*)?$",
        re.IGNORECASE,
    )),

    # Home page
    ("home", re.compile(
        r"^https?://(?:www\.)?hepsiburada\.com/?$",
        re.IGNORECASE,
    )),
]

def classify_url(url: str) -> str:
    """Return a category label for a given URL or 'other' if no pattern matches."""
    u = (url or "").strip()
    for label, pat in PATTERNS:
        if pat.match(u):
            return label
    return "other"

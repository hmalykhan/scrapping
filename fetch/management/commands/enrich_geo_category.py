# fetch/management/commands/enrich_geo_category.py
"""
Cron/terminal friendly geo enrichment with resume + per-row logging.

Key fixes vs older version:
- Avoids QuerySet.iterator() server-side cursors (prevents "cursor already closed")
- Infers country for Channel Islands / Isle of Man postcodes (JE/GY/IM)
- Handles multi-location text blobs by extracting first postcode line
- Skips vague non-address strings (UK/remote/nationally/etc) unless a postcode exists
- Row logging enabled by default (disable with --no-row-log)
- Postcode-only fallback when full address returns empty
"""

import json
import re
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections
from django.db.models import Q

from job.models import DwpJob
from course.models import NcsCourse
from apprenticeship.models import ApprenticeshipVacancy


GEOAPIFY_URL = "https://api.geoapify.com/v1/geocode/search"

UK_POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?)\s*(\d[A-Z]{2})\b", re.IGNORECASE)
ADMIN_WORDS_RE = re.compile(
    r"\b(borough|district|county|metropolitan|unitary|authority|parish|region)\b", re.IGNORECASE
)
IGNORE_TOKENS_RE = re.compile(
    r"^(united kingdom|uk|england|scotland|wales|northern ireland|great britain)$", re.IGNORECASE
)

# Map special postcode prefixes to ISO2 country codes (Geoapify expects lowercase ISO2)
POSTCODE_PREFIX_COUNTRY = {
    "JE": "je",  # Jersey
    "GY": "gg",  # Guernsey
    "IM": "im",  # Isle of Man
}

# Vague non-location tokens (skip unless we can extract a postcode)
VAGUE_TOKENS = {
    "uk",
    "united kingdom",
    "great britain",
    "remote",
    "flexible",
    "recruiting nationally",
    "nationally",
    "national",
    "nationwide",
}


# ------------------------ Helpers ------------------------

def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def extract_uk_postcode(s: str) -> str:
    s = (s or "").strip()
    m = UK_POSTCODE_RE.search(s)
    if not m:
        return ""
    outward = m.group(1).upper().strip()
    inward = m.group(2).upper().strip()
    return f"{outward} {inward}"


def outward_code(postcode: str) -> str:
    pc = (postcode or "").strip().upper()
    if not pc:
        return ""
    m = UK_POSTCODE_RE.search(pc)
    if not m:
        return pc.split(" ")[0]
    return m.group(1).upper().strip()


def looks_like_admin_area(name: str) -> bool:
    s = (name or "").strip()
    if not s:
        return False
    return bool(ADMIN_WORDS_RE.search(s))


def clean_city(candidate: str) -> str:
    """
    Generic cleaner:
    - keeps part before comma
    - strips admin prefixes
    - removes trailing postcode
    - removes trailing (...) or [...]
    """
    s = (candidate or "").strip()
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""

    if "," in s:
        s = s.split(",", 1)[0].strip()

    prefixes = [
        "london borough of",
        "metropolitan borough of",
        "royal borough of",
        "borough of",
        "district of",
        "county of",
        "city of",
        "unitary authority of",
        "municipality of",
        "region of",
    ]
    for p in prefixes:
        s = re.sub(rf"^{re.escape(p)}\s+", "", s, flags=re.IGNORECASE).strip()

    s = re.sub(r"\s*[\(\[].*?[\)\]]\s*$", "", s).strip()
    s = re.sub(UK_POSTCODE_RE, "", s).strip()

    if IGNORE_TOKENS_RE.fullmatch(s or ""):
        return ""

    return s


def extract_locality_from_address(address: str) -> str:
    """
    Best-guess locality from input text:
      "Catford, SE6 9SE" -> "Catford"
      multi-line -> first meaningful token
    """
    s = (address or "").strip()
    if not s:
        return ""

    parts: List[str] = []
    for p in re.split(r"[\n,]+", s):
        p = re.sub(r"\s+", " ", p).strip()
        if not p:
            continue
        # skip if it’s just postcode
        if UK_POSTCODE_RE.fullmatch(p.replace(" ", "")) or UK_POSTCODE_RE.fullmatch(p):
            continue
        # skip country words
        if IGNORE_TOKENS_RE.fullmatch(p):
            continue
        parts.append(p)

    return parts[0] if parts else ""


def pick_best_city(res: Dict[str, Any], address: str) -> str:
    """
    Prefer locality-like fields. If admin-ish, fall back to parsing address.
    """
    candidates = [
        res.get("suburb"),
        res.get("neighbourhood"),
        res.get("hamlet"),
        res.get("village"),
        res.get("town"),
        res.get("city"),
    ]
    cleaned = [clean_city(c or "") for c in candidates if c]

    for c in cleaned:
        if c and not looks_like_admin_area(c):
            return c

    addr_locality = clean_city(extract_locality_from_address(address))
    if addr_locality and not looks_like_admin_area(addr_locality):
        return addr_locality

    if addr_locality:
        return addr_locality

    for c in cleaned:
        if c:
            return c

    return ""


def has_geo(obj) -> bool:
    return bool(
        obj.city and obj.state and obj.zip_code and
        obj.latitude is not None and obj.longitude is not None
    )


def save_row_safely(obj, update_fields: List[str], retries: int = 3) -> bool:
    for attempt in range(1, retries + 1):
        try:
            connections["default"].close_if_unusable_or_obsolete()
            obj.save(update_fields=update_fields)
            return True
        except Exception:
            if attempt == retries:
                return False
            time.sleep(0.5 * attempt)
    return False


def infer_country_from_text(text: str, default: str = "gb") -> str:
    """
    If postcode starts with JE/GY/IM use those country codes for Geoapify filter.
    Otherwise default to gb.
    """
    pc = extract_uk_postcode(text)
    if not pc:
        return default
    prefix2 = pc[:2].upper()
    return POSTCODE_PREFIX_COUNTRY.get(prefix2, default)


def normalize_postcode_in_text(text: str) -> str:
    """
    If text contains a postcode without space (EX314JB) -> normalize to "EX31 4JB".
    We replace the first found match (good enough for these use cases).
    """
    s = (text or "").strip()
    m = UK_POSTCODE_RE.search(s)
    if not m:
        return s
    full = m.group(0)
    formatted = f"{m.group(1).upper().strip()} {m.group(2).upper().strip()}"
    return s.replace(full, formatted, 1)


def pick_geocode_text(raw: str) -> str:
    """
    Choose a better text input for geocoding.
    - Multi-line blobs: pick first line containing a postcode
    - Skip vague tokens unless we can extract a postcode
    - Normalize postcode spacing
    """
    s = (raw or "").strip()
    s = re.sub(r"\s+\n", "\n", s)
    s = re.sub(r"\n\s+", "\n", s)
    s = re.sub(r"\s+", " ", s).strip() if "\n" not in s else s.strip()

    if not s:
        return ""

    # Multi-line: pick first line with postcode
    if "\n" in s:
        for line in s.splitlines():
            line = line.strip()
            if not line:
                continue
            if extract_uk_postcode(line):
                return normalize_postcode_in_text(line)
        # fallback: first non-empty line
        for line in s.splitlines():
            line = line.strip()
            if line:
                return normalize_postcode_in_text(line)

    # Single-line
    low = s.strip().lower().strip(",")
    if not extract_uk_postcode(s) and low in VAGUE_TOKENS:
        return ""

    return normalize_postcode_in_text(s)


# ------------------------ Settings / Keys ------------------------

def get_geoapify_keys() -> List[str]:
    """
    Supports:
      GEOAPIFY_API_KEY="KEY"
      GEOAPIFY_API_KEY=["KEY1","KEY2",...]
    """
    raw = getattr(settings, "GEOAPIFY_API_KEY", None)
    if isinstance(raw, (list, tuple)):
        keys = [str(k).strip() for k in raw if k and str(k).strip()]
    else:
        keys = [str(raw).strip()] if raw and str(raw).strip() else []

    if not keys:
        raise RuntimeError("Missing GEOAPIFY_API_KEY in Django settings/environment")
    return keys


class RateLimiter:
    def __init__(self, rps: float = 5.0):
        if rps <= 0:
            raise ValueError("rps must be > 0")
        self.min_interval = 1.0 / float(rps)
        self._next_time = 0.0

    def wait(self):
        now = time.monotonic()
        if self._next_time <= 0:
            self._next_time = now
        if now < self._next_time:
            time.sleep(self._next_time - now)
        self._next_time = max(self._next_time, time.monotonic()) + self.min_interval


def _looks_like_quota_or_rate_limit(resp: Optional[requests.Response]) -> bool:
    if resp is None:
        return False
    return resp.status_code in (429, 403)


def geoapify_geocode(
    session: requests.Session,
    address: str,
    api_key: str,
    country_code: str = "gb",
    timeout: int = 20,
) -> Dict[str, Any]:
    address = (address or "").strip()
    if not address:
        return {}

    params = {
        "text": address,
        "format": "json",
        "apiKey": api_key,
        "limit": 1,
        "filter": f"countrycode:{country_code}",
    }

    r = session.get(GEOAPIFY_URL, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    results = data.get("results") or []
    if not results:
        return {}

    res = results[0]

    state = (res.get("state") or "").strip()
    zip_code = (res.get("postcode") or "").strip()
    city = pick_best_city(res, address)

    # sanity check: avoid totally wrong matches (outward code mismatch)
    input_pc = extract_uk_postcode(address)
    if input_pc and zip_code:
        if outward_code(input_pc) and outward_code(zip_code) and outward_code(input_pc) != outward_code(zip_code):
            return {}

    lat = res.get("lat")
    lon = res.get("lon")

    latitude = Decimal(str(lat)) if lat is not None else None
    longitude = Decimal(str(lon)) if lon is not None else None

    return {
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "latitude": latitude,
        "longitude": longitude,
    }


class GeoapifyKeyRotator:
    """
    Rotates keys after `per_key_limit` calls and on 429/403.
    Enforces max `rps` using RateLimiter.
    Adds postcode-only fallback when full address geocode returns empty.
    """
    def __init__(self, keys: List[str], per_key_limit: int = 10000, rps: float = 5.0, timeout: int = 20):
        self.keys = keys
        self.per_key_limit = per_key_limit
        self.idx = 0
        self.usage = {k: 0 for k in keys}
        self.limiter = RateLimiter(rps=rps)
        self.timeout = timeout
        self.session = requests.Session()

    def _advance(self):
        self.idx += 1
        if self.idx >= len(self.keys):
            raise RuntimeError(f"All Geoapify keys exhausted. Usage: {self.usage}")

    def _current_key(self) -> str:
        k = self.keys[self.idx]
        if self.per_key_limit and self.usage[k] >= self.per_key_limit:
            self._advance()
            return self._current_key()
        return k

    def geocode(self, address: str, country: str) -> Dict[str, Any]:
        """
        1) Try full address
        2) If empty and a postcode exists, try postcode-only
        """
        while True:
            k = self._current_key()
            try:
                # full address
                self.limiter.wait()
                out = geoapify_geocode(
                    session=self.session,
                    address=address,
                    api_key=k,
                    country_code=country,
                    timeout=self.timeout,
                )
                self.usage[k] += 1

                if out:
                    return out

                # fallback: postcode-only
                pc = extract_uk_postcode(address)
                if pc and pc.strip().lower() != address.strip().lower():
                    self.limiter.wait()
                    out2 = geoapify_geocode(
                        session=self.session,
                        address=pc,
                        api_key=k,
                        country_code=country,
                        timeout=self.timeout,
                    )
                    self.usage[k] += 1
                    return out2 or {}

                return {}

            except requests.HTTPError as e:
                resp = getattr(e, "response", None)
                if _looks_like_quota_or_rate_limit(resp):
                    self._advance()
                    continue
                raise


# ------------------------ Categories + State ------------------------

def load_categories(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"categories.json not found at: {path}")

    raw = json.loads(p.read_text(encoding="utf-8"))

    if isinstance(raw, dict):
        cats = [k.strip() for k in raw.keys() if isinstance(k, str) and k.strip()]
    elif isinstance(raw, list):
        cats = [x.strip() for x in raw if isinstance(x, str) and x.strip()]
    else:
        cats = []

    seen = set()
    out = []
    for c in cats:
        if c not in seen:
            seen.add(c)
            out.append(c)

    if not out:
        raise RuntimeError("No categories found in categories.json.")
    return out


def default_state_path() -> Path:
    base_dir = getattr(settings, "BASE_DIR", None)
    if base_dir:
        return Path(base_dir) / "geo_enrich_state.json"
    return Path.cwd() / "geo_enrich_state.json"


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": 1, "categories": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        bak = path.with_suffix(".corrupt.bak")
        try:
            bak.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            pass
        return {"version": 1, "categories": {}}


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def get_cat_state(state: Dict[str, Any], category: str) -> Dict[str, Any]:
    cats = state.setdefault("categories", {})
    return cats.setdefault(category, {
        "job": {"last_pk": 0, "retry_pks": []},
        "course": {"last_pk": 0, "retry_pks": []},
        "apprenticeship": {"last_pk": 0, "retry_pks": []},
    })


def push_retry(cat_state: Dict[str, Any], kind: str, pk: int, max_retry: int = 5000) -> None:
    bucket = cat_state[kind].setdefault("retry_pks", [])
    if pk in bucket:
        return
    bucket.append(pk)
    if len(bucket) > max_retry:
        del bucket[0:len(bucket) - max_retry]


def pop_retry_batch(cat_state: Dict[str, Any], kind: str, n: int) -> List[int]:
    bucket = cat_state[kind].setdefault("retry_pks", [])
    if not bucket:
        return []
    batch = bucket[:n]
    del bucket[:n]
    return batch


def missing_geo_q() -> Q:
    return Q(latitude__isnull=True) | Q(longitude__isnull=True) | Q(city="") | Q(state="") | Q(zip_code="")


# ------------------------ Command ------------------------

class Command(BaseCommand):
    help = "Geo enrichment for all categories with resume/checkpoint + key rotation + per-row logging."

    def add_arguments(self, parser):
        parser.add_argument("--categories-file", default="/var/www/scrapping/categories.json")
        parser.add_argument("--state-file", default="")
        parser.add_argument(
            "--country",
            default="gb",
            help="Default country filter (gb). JE/GG/IM inferred automatically from postcode.",
        )

        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--limit", type=int, default=0)
        parser.add_argument("--retry-first", type=int, default=200)

        parser.add_argument("--per-key-limit", type=int, default=3000)
        parser.add_argument("--rps", type=float, default=5.0)
        parser.add_argument("--timeout", type=int, default=20)

        parser.add_argument("--progress-every", type=int, default=500)
        parser.add_argument("--db-retries", type=int, default=3)
        parser.add_argument("--flush-state-every", type=int, default=50)
        parser.add_argument("--fetch-chunk", type=int, default=1000, help="Fetch PKs in chunks (default: 1000)")

        # Row logging ON by default; disable with --no-row-log
        parser.add_argument(
            "--no-row-log",
            action="store_false",
            dest="row_log",
            default=True,
            help="Disable per-row log lines",
        )

    def handle(self, *args, **opts):
        categories_file: str = opts["categories_file"]
        default_country: str = opts["country"]
        dry_run: bool = opts["dry_run"]
        limit: int = int(opts["limit"] or 0)
        retry_first: int = int(opts["retry_first"] or 0)

        per_key_limit: int = int(opts["per_key_limit"])
        rps: float = float(opts["rps"])
        timeout: int = int(opts["timeout"])
        progress_every: int = max(1, int(opts["progress_every"]))
        db_retries: int = max(1, int(opts["db_retries"]))
        flush_state_every: int = max(1, int(opts["flush_state_every"]))
        fetch_chunk: int = max(50, int(opts["fetch_chunk"]))
        row_log: bool = bool(opts["row_log"])

        state_path = Path(opts["state_file"]).expanduser() if opts["state_file"] else default_state_path()
        state = load_state(state_path)

        categories = load_categories(categories_file)
        keys = get_geoapify_keys()
        rotator = GeoapifyKeyRotator(keys, per_key_limit=per_key_limit, rps=rps, timeout=timeout)

        self.stdout.write(self.style.SUCCESS(f"Loaded categories={len(categories)} from {categories_file}"))
        self.stdout.write(
            self.style.SUCCESS(
                f"Keys={len(keys)} | per_key_limit={per_key_limit} | max_rps={rps} | "
                f"state_file={state_path} | row_log={row_log}"
            )
        )

        # cache reduces API calls; include country to avoid collisions
        addr_cache: Dict[str, Dict[str, Any]] = {}
        update_fields = ["city", "state", "zip_code", "latitude", "longitude"]

        processed_total = 0
        updated_total = {"job": 0, "course": 0, "apprenticeship": 0}
        skipped_geo_total = {"job": 0, "course": 0, "apprenticeship": 0}
        skipped_empty_total = {"job": 0, "course": 0, "apprenticeship": 0}
        empty_geo_total = 0
        failed_geocode_total = 0
        failed_save_total = 0

        def log_row(kind: str, category: str, pk: int, status: str, addr: str):
            if not row_log:
                return
            a = (addr or "").replace("\n", " | ")
            if len(a) > 180:
                a = a[:180] + "…"
            self.stdout.write(f"[row] {kind} cat='{category}' id={pk} status={status} addr='{a}'")

        def get_address(kind: str, obj) -> str:
            if kind == "job":
                return getattr(obj, "location", "") or ""
            if kind == "course":
                return getattr(obj, "address", "") or ""
            # apprenticeship
            return (getattr(obj, "where_youll_work_address", "") or "") or (getattr(obj, "location_summary", "") or "")

        def queryset_for(kind: str):
            if kind == "job":
                return DwpJob.objects
            if kind == "course":
                return NcsCourse.objects
            return ApprenticeshipVacancy.objects

        def process_one(kind: str, category: str, obj, cat_state: Dict[str, Any], kind_state: Dict[str, Any]):
            nonlocal processed_total, empty_geo_total, failed_geocode_total, failed_save_total

            pk = int(obj.pk)
            processed_total += 1

            # keep connection healthy in long runs
            if processed_total % 500 == 0:
                connections["default"].close_if_unusable_or_obsolete()

            if has_geo(obj):
                skipped_geo_total[kind] += 1
                kind_state["last_pk"] = max(int(kind_state.get("last_pk") or 0), pk)
                log_row(kind, category, pk, "skip_has_geo", get_address(kind, obj))
                return

            raw_addr = get_address(kind, obj)
            addr = pick_geocode_text(raw_addr)
            if not addr:
                skipped_empty_total[kind] += 1
                kind_state["last_pk"] = max(int(kind_state.get("last_pk") or 0), pk)
                log_row(kind, category, pk, "skip_no_usable_address", raw_addr)
                return

            country_code = infer_country_from_text(addr, default=default_country)
            cache_key = f"{country_code}:{normalize(addr)}"

            if cache_key not in addr_cache:
                try:
                    addr_cache[cache_key] = rotator.geocode(address=addr, country=country_code) or {}
                except Exception as e:
                    failed_geocode_total += 1
                    push_retry(cat_state, kind, pk)
                    kind_state["last_pk"] = max(int(kind_state.get("last_pk") or 0), pk)
                    log_row(kind, category, pk, f"geocode_error_retry({type(e).__name__})", addr)
                    return

            geo = addr_cache.get(cache_key) or {}
            if not geo:
                empty_geo_total += 1
                push_retry(cat_state, kind, pk)
                kind_state["last_pk"] = max(int(kind_state.get("last_pk") or 0), pk)
                log_row(kind, category, pk, "geocode_empty_retry", addr)
                return

            obj.city = geo["city"]
            obj.state = geo["state"]
            obj.zip_code = geo["zip_code"]
            obj.latitude = geo["latitude"]
            obj.longitude = geo["longitude"]

            if dry_run:
                updated_total[kind] += 1
                kind_state["last_pk"] = max(int(kind_state.get("last_pk") or 0), pk)
                log_row(kind, category, pk, "dryrun_updated", addr)
                return

            ok = save_row_safely(obj, update_fields=update_fields, retries=db_retries)
            if ok:
                updated_total[kind] += 1
                kind_state["last_pk"] = max(int(kind_state.get("last_pk") or 0), pk)
                log_row(kind, category, pk, "updated", addr)
            else:
                failed_save_total += 1
                push_retry(cat_state, kind, pk)
                kind_state["last_pk"] = max(int(kind_state.get("last_pk") or 0), pk)
                log_row(kind, category, pk, "db_save_failed_retry", addr)

        def process_kind(kind: str, category: str, base_qs):
            """
            Cursor-safe processing:
            - Retry batch first (by pk list)
            - Forward scan by pk-chunks (no iterator cursor)
            """
            cat_state = get_cat_state(state, category)
            kind_state = cat_state[kind]
            last_pk = int(kind_state.get("last_pk") or 0)

            # 1) Retry batch first
            retry_ids = pop_retry_batch(cat_state, kind, retry_first) if retry_first > 0 else []
            if retry_ids:
                objs = list(
                    queryset_for(kind)
                    .filter(pk__in=retry_ids, category=category)
                    .filter(missing_geo_q())
                    .order_by("pk")
                )
                for obj in objs:
                    process_one(kind, category, obj, cat_state, kind_state)
                    if processed_total % flush_state_every == 0:
                        save_state(state_path, state)

            # 2) Forward scan by pk-chunks
            scan_qs = base_qs.filter(pk__gt=last_pk).order_by("pk")

            processed_in_kind = 0
            cursor = last_pk

            while True:
                if limit and processed_in_kind >= limit:
                    break

                take = fetch_chunk
                if limit:
                    take = min(take, max(0, limit - processed_in_kind))
                    if take <= 0:
                        break

                pks = list(scan_qs.filter(pk__gt=cursor).values_list("pk", flat=True)[:take])
                if not pks:
                    break

                objs = list(queryset_for(kind).filter(pk__in=pks).order_by("pk"))
                for obj in objs:
                    process_one(kind, category, obj, cat_state, kind_state)
                    processed_in_kind += 1
                    cursor = int(obj.pk)

                    if processed_total % progress_every == 0:
                        self.stdout.write(
                            f"Progress total={processed_total} | cache={len(addr_cache)} "
                            f"| updated(j/c/a)={updated_total['job']}/{updated_total['course']}/{updated_total['apprenticeship']} "
                            f"| key_usage={rotator.usage} | state_saved_every={flush_state_every}"
                        )

                    if processed_total % flush_state_every == 0:
                        save_state(state_path, state)

            save_state(state_path, state)

        # -------- Process all categories --------
        for idx, category in enumerate(categories, start=1):
            jobs_qs = DwpJob.objects.filter(category=category).filter(missing_geo_q())
            courses_qs = NcsCourse.objects.filter(category=category).filter(missing_geo_q())
            apps_qs = ApprenticeshipVacancy.objects.filter(category=category).filter(missing_geo_q())

            j_cnt = jobs_qs.count()
            c_cnt = courses_qs.count()
            a_cnt = apps_qs.count()

            if (j_cnt + c_cnt + a_cnt) == 0:
                get_cat_state(state, category)
                continue

            self.stdout.write(
                self.style.SUCCESS(
                    f"[{idx}/{len(categories)}] '{category}' missing_geo: jobs={j_cnt} courses={c_cnt} apps={a_cnt}"
                )
            )

            process_kind("job", category, jobs_qs)
            process_kind("course", category, courses_qs)
            process_kind("apprenticeship", category, apps_qs)

            save_state(state_path, state)

        save_state(state_path, state)

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ DONE | processed_total={processed_total} | cache_size={len(addr_cache)} | "
                f"updated={updated_total} | skipped_geo={skipped_geo_total} | skipped_empty={skipped_empty_total} | "
                f"empty_geo={empty_geo_total} | failed_geocode={failed_geocode_total} | failed_save={failed_save_total}"
            )
        )
        self.stdout.write(self.style.SUCCESS(f"✅ Key usage: {rotator.usage}"))

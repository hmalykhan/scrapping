"""
Career card images: women only for female-related careers, men for the rest.

The image prompt never said who should be in the picture, and Gemini chose
women for most careers - 13 of a 16-career sample, including electrician-
adjacent trades, pilots and engineers. The client wants female-related
careers (nursing, midwifery, beauty, gynaecology ...) to show a woman and
every other career a man.

Careers only. Job, apprenticeship and course images are not used by the app
and are not touched.

Steps, each safe to re-run:

  --step classify   Decide female/male per career (Gemini text). Writes
                    targets.json for the client to review and edit. No DB write.
  --step audit      Look at the image each career shows today and record
                    man / woman / unclear (Gemini vision). No DB write.
  --step sample     Generate N corrected images to local files only, so the
                    style can be checked. No upload, no DB write.
  --step generate   (after sign-off) regenerate mismatches, upload under NEW
                    Spaces keys, point every row of the career at the new
                    image. Old URLs saved to undo.json first.
  --step undo       Put every changed row back to its old URL.

One image per career (job_slug), on all its rows: the app shows the lowest-id
row within each user's categories, so different users can land on different
rows of the same career.
"""
import base64
import io
import json
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from decouple import config
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

DATA = Path(__file__).resolve().parents[2] / "data" / "career_image_gender"
TARGETS = DATA / "targets.json"
AUDIT = DATA / "audit.json"
UNDO = DATA / "undo.json"
SAMPLES = DATA / "samples"

CLASSIFY_PROMPT = """You score UK careers for a careers app's picture cards.

For each career give "female_score" from 0 to 100: how strongly the career is
a female-related one - work mostly done by or for women. Examples near 100:
midwife, nurse, beauty therapist, nail technician, nanny, gynaecologist.
Examples near 0: electrician, bricklayer, pilot, engineer, soldier.

Return one entry for every career below, with the name copied exactly.

Careers:
{names}
"""

AUDIT_PROMPT = """Look at the main person in this photo - the one the photo is about.
Is that person a man or a woman? Answer "unclear" only if there is no clear
main person or you genuinely cannot tell."""

RETRYABLE = ("503", "UNAVAILABLE", "429", "RESOURCE_EXHAUSTED", "500",
             "INTERNAL", "deadline", "timeout")


def career_image_prompt(jobname: str, gender: str, insist: bool = False) -> str:
    """
    The original prompt, plus who is in the picture. Style unchanged.

    `insist` is for retries: a few careers (chiropractor) came back with the
    wrong person three times running, because the model's own association is
    strong. It only repeats the requirement, it does not change the style.
    """
    jobname = " ".join((jobname or "").split())[:240]
    person = "a woman" if gender == "female" else "a man"
    if insist:
        person = ("%s - this is required. Do NOT show %s as the main person."
                  % (person, "a man" if gender == "female" else "a woman"))
    return (
        "Photorealistic lifestyle photo for a jobs/courses recommendation app thumbnail.\n"
        f"JOB ROLE/THEME: {jobname}\n"
        f"MAIN PERSON: {person}, clearly the focus, realistically doing this job "
        "with the correct uniform, equipment and workplace for it.\n"
        f"Any other people in the scene are in the background.\n\n"
        "STYLE:\n"
        "- Natural lighting, neutral white balance, true-to-life colors\n"
        "- Clean, modern, realistic (no tint, no filters, no heavy grading)\n"
        "- Real-world candid scene, shallow depth of field\n"
        "- Square 1:1 composition, centered subject\n\n"
        "AVOID:\n"
        "- Text, captions, words, letters, typography\n"
        "- Logos, watermarks, brand names\n"
        "- UI overlays, app screens, icons, badges\n"
        "- Posters/signage with readable text\n"
        "- Heavy color filters, pink/red tint, gradient overlays\n"
    )


def _retry(fn, attempts=5):
    last = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            last = e
            if not any(m in str(e) for m in RETRYABLE) or i == attempts - 1:
                raise
            time.sleep((2 ** i) + random.uniform(0, 1))
    raise last


def careers():
    """One row per career: job_slug, name, and the image its lowest-id row shows."""
    with connection.cursor() as c:
        c.execute("""
            select distinct on (job_slug) job_slug, jobname, dg_image_url
            from fetch_careerjob
            where coalesce(job_slug, '') <> ''
            order by job_slug, id
        """)
        return [{"slug": s, "name": n, "image": u or ""} for s, n, u in c.fetchall()]


class Command(BaseCommand):
    help = "Career images: women for female-related careers, men for the rest."

    def add_arguments(self, p):
        p.add_argument("--step", required=True,
                       choices=["classify", "audit", "sample", "generate", "unify", "undo"])
        p.add_argument("--limit", type=int, default=0)
        p.add_argument("--workers", type=int, default=6)
        p.add_argument("--text-model", default="gemini-2.5-flash")
        p.add_argument("--female-share", type=float, default=0.10,
                       help="share of careers shown with a woman (client: 90:10)")
        p.add_argument("--min-score", type=int, default=None,
                       help="female = score >= this (overrides --female-share). Client chose 90.")
        p.add_argument("--reuse-scores", action="store_true",
                       help="re-draw the line from the scores already in targets.json, no API calls")
        # Explicit, not GEMINI_IMAGE_MODEL: that is set to '' in .env, and
        # os.getenv returns '' rather than the default - a 404 on every call.
        # This is the model that made the existing images, so the look matches.
        p.add_argument("--image-model", default="gemini-2.5-flash-image")
        p.add_argument("--extra", action="store_true",
                       help="generate: the leftovers instead - careers whose image was 'unclear' "
                            "and careers the audit never saw (a row with no image, or a failed check)")

    def handle(self, *a, **o):
        DATA.mkdir(parents=True, exist_ok=True)
        key = config("GEMINI_API_KEY", default="")
        if not key:
            raise CommandError("GEMINI_API_KEY is not set")
        from google import genai
        # Held for the whole run: an unreferenced client can be closed mid-request.
        self.client = genai.Client(api_key=key)
        self.o = o
        getattr(self, "_" + o["step"])()

    # -- classify -----------------------------------------------------------
    def _classify(self):
        """
        Score every career, then the top --female-share become female.

        A ratio, not a yes/no per career: the client wants 85:15 men to women,
        and an independent yes/no gave 9% with inconsistent neighbours
        (school secretary female, legal secretary male).
        """
        from google.genai import types
        rows = careers()
        names = sorted({r["name"] for r in rows})
        if self.o["reuse_scores"]:
            prev = json.loads(TARGETS.read_text())
            scores = {v["career"]: v["score"] for v in prev.values()}
            missing = [n for n in names if n not in scores]
            if missing:
                raise CommandError("no saved score for %d careers, e.g. %s" % (len(missing), missing[:3]))
            return self._draw_line(rows, names, scores)
        schema = {"type": "array", "items": {"type": "object", "properties": {
            "career": {"type": "string"},
            "female_score": {"type": "integer"}},
            "required": ["career", "female_score"]}}
        scores = {}
        batches = [names[i:i + 60] for i in range(0, len(names), 60)]
        for n, batch in enumerate(batches, 1):
            def call():
                r = self.client.models.generate_content(
                    model=self.o["text_model"],
                    contents=CLASSIFY_PROMPT.format(names="\n".join(batch)),
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=schema, temperature=0.0))
                return json.loads(r.text)
            got = {d["career"]: max(0, min(100, int(d["female_score"]))) for d in _retry(call)}
            for name in batch:
                scores[name] = got.get(name, 0)      # skipped or renamed -> male
            self.stdout.write("  batch %d/%d" % (n, len(batches)))

        return self._draw_line(rows, names, scores)

    def _draw_line(self, rows, names, scores):
        # Rank careers (not rows) and take the top share. Ties broken by name
        # so re-running gives the same list.
        ranked = sorted(names, key=lambda x: (-scores[x], x))
        if self.o["min_score"] is not None:
            # A score line never splits careers with equal scores; a share can.
            n_female = sum(1 for x in names if scores[x] >= self.o["min_score"])
        else:
            n_female = round(len(ranked) * self.o["female_share"])
        female = set(ranked[:n_female])

        by_slug = {r["slug"]: {"career": r["name"], "score": scores[r["name"]],
                               "gender": "female" if r["name"] in female else "male"}
                   for r in rows}
        TARGETS.write_text(json.dumps(by_slug, indent=1, ensure_ascii=False))
        cutoff = scores[ranked[n_female - 1]] if n_female else None
        self.stdout.write(self.style.SUCCESS(
            "%d careers: %d female (%.0f%%), %d male; lowest female score %s -> %s"
            % (len(names), n_female, 100.0 * n_female / len(names),
               len(names) - n_female, cutoff, TARGETS)))

    # -- audit --------------------------------------------------------------
    def _audit(self):
        import requests
        from PIL import Image
        from google.genai import types
        rows = [r for r in careers() if r["image"]]
        if self.o["limit"]:
            rows = rows[: self.o["limit"]]
        done = json.loads(AUDIT.read_text()) if AUDIT.exists() else {}
        todo = [r for r in rows if done.get(r["slug"], {}).get("image") != r["image"]]
        schema = {"type": "object", "properties": {
            "person": {"type": "string", "enum": ["man", "woman", "unclear"]}},
            "required": ["person"]}

        def look(r):
            raw = requests.get(r["image"], timeout=60).content
            im = Image.open(io.BytesIO(raw)).convert("RGB")
            im.thumbnail((512, 512))
            buf = io.BytesIO(); im.save(buf, "JPEG", quality=85)

            def call():
                resp = self.client.models.generate_content(
                    model=self.o["text_model"],
                    contents=[types.Part.from_bytes(data=buf.getvalue(), mime_type="image/jpeg"),
                              AUDIT_PROMPT],
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=schema, temperature=0.0))
                return json.loads(resp.text)["person"]
            return r, _retry(call)

        with ThreadPoolExecutor(self.o["workers"]) as ex:
            futs = [ex.submit(look, r) for r in todo]
            for i, f in enumerate(as_completed(futs), 1):
                try:
                    r, person = f.result()
                    done[r["slug"]] = {"career": r["name"], "image": r["image"], "person": person}
                except Exception as e:
                    self.stderr.write("  failed: %s" % e)
                if i % 50 == 0:
                    AUDIT.write_text(json.dumps(done, indent=1, ensure_ascii=False))
                    self.stdout.write("  %d/%d" % (i, len(todo)))
        AUDIT.write_text(json.dumps(done, indent=1, ensure_ascii=False))
        counts = {}
        for v in done.values():
            counts[v["person"]] = counts.get(v["person"], 0) + 1
        self.stdout.write(self.style.SUCCESS("audited %d: %s -> %s" % (len(done), counts, AUDIT)))

    # -- sample -------------------------------------------------------------
    def _sample(self):
        from fetch.services.image_job import _gemini_generate_image
        if not TARGETS.exists():
            raise CommandError("run --step classify first")
        targets = json.loads(TARGETS.read_text())
        SAMPLES.mkdir(exist_ok=True)
        picks = self.o["limit"] or 6
        fem = [v for v in targets.values() if v["gender"] == "female"]
        male = [v for v in targets.values() if v["gender"] == "male"]
        random.seed(7)
        chosen = random.sample(fem, min(len(fem), picks // 3)) + \
                 random.sample(male, picks - min(len(fem), picks // 3))
        for v in chosen:
            img = _retry(lambda: _gemini_generate_image(career_image_prompt(v["career"], v["gender"]),
                                                     model=self.o["image_model"]))
            fn = SAMPLES / ("%s_%s.%s" % (v["gender"], v["career"].replace(" ", "_").replace("/", "-")[:40],
                                          img.mime_type.split("/")[-1]))
            fn.write_bytes(base64.b64decode(img.data_b64))
            self.stdout.write("  %s" % fn.name)
        self.stdout.write(self.style.SUCCESS("samples in %s (nothing uploaded, DB untouched)" % SAMPLES))

    # -- generate -----------------------------------------------------------
    def _guard_backup_db(self):
        host = connection.settings_dict.get("HOST", "")
        if "sparkling-wave" not in host:
            raise CommandError("REFUSING: database is not the backup (sparkling-wave): %r" % host)

    def _spaces(self):
        import boto3
        from botocore.client import Config
        need = ["DO_SPACES_KEY", "DO_SPACES_SECRET", "DO_SPACES_REGION",
                "DO_SPACES_BUCKET", "DO_SPACES_ENDPOINT"]
        v = {k: config(k, default="") for k in need}
        missing = [k for k in need if not v[k]]
        if missing:
            raise CommandError("missing %s" % ", ".join(missing))
        s3 = boto3.client("s3", region_name=v["DO_SPACES_REGION"], endpoint_url=v["DO_SPACES_ENDPOINT"],
                          aws_access_key_id=v["DO_SPACES_KEY"], aws_secret_access_key=v["DO_SPACES_SECRET"],
                          config=Config(signature_version="s3v4"))
        # The public base is taken from links that already work, NOT from
        # DO_SPACES_CDN_BASE: in .env that reads
        #   https://pathzi.lon1.cdn.pathzi/digitaloceanspaces.com   (malformed)
        # and the pilot wrote exactly that broken host into the database.
        # Working links look like .../pathzi/career-images/..., because the
        # endpoint already names the bucket and uploads land under "pathzi/".
        with connection.cursor() as c:
            c.execute("select dg_image_url from fetch_careerjob "
                      "where dg_image_url like %s limit 1", ["%/career-images/%"])
            row = c.fetchone()
        if not row:
            raise CommandError("no existing Spaces link to take the URL format from")
        base = row[0].split("/career-images/")[0]
        return s3, v["DO_SPACES_BUCKET"], base

    def _looks_like(self, png_bytes):
        from PIL import Image
        from google.genai import types
        im = Image.open(io.BytesIO(png_bytes)).convert("RGB"); im.thumbnail((512, 512))
        buf = io.BytesIO(); im.save(buf, "JPEG", quality=85)
        schema = {"type": "object", "properties": {
            "person": {"type": "string", "enum": ["man", "woman", "unclear"]}}, "required": ["person"]}

        def call():
            r = self.client.models.generate_content(
                model=self.o["text_model"],
                contents=[types.Part.from_bytes(data=buf.getvalue(), mime_type="image/jpeg"), AUDIT_PROMPT],
                config=types.GenerateContentConfig(response_mime_type="application/json",
                                                   response_schema=schema, temperature=0.0))
            return json.loads(r.text)["person"]
        return _retry(call)

    def _generate(self):
        import threading
        from fetch.services.image_job import _gemini_generate_image
        self._guard_backup_db()
        targets = json.loads(TARGETS.read_text())
        audit = json.loads(AUDIT.read_text())
        undo = json.loads(UNDO.read_text()) if UNDO.exists() else {}
        want = {"female": "woman", "male": "man"}

        # Only careers whose CURRENT image shows the wrong person. Unclear,
        # un-audited and image-less careers are left alone and reported.
        if self.o["extra"]:
            todo = []
            for slug, v in targets.items():
                if slug in undo:
                    continue
                if audit.get(slug, {}).get("person") == "unclear":
                    todo.append((slug, v))
                elif slug not in audit:
                    # Never audited. Replace unless EVERY row already has an
                    # image showing the right person.
                    with connection.cursor() as c:
                        c.execute("select distinct coalesce(dg_image_url,'') from fetch_careerjob "
                                  "where job_slug = %s", [slug])
                        urls = [r[0] for r in c.fetchall()]
                    if "" in urls or len(urls) != 1:
                        todo.append((slug, v))
                        continue
                    import requests
                    person = self._looks_like(requests.get(urls[0], timeout=60).content)
                    self.stdout.write("  %s: current image shows a %s" % (v["career"], person))
                    if person != want[v["gender"]]:
                        todo.append((slug, v))
        else:
            todo = [(slug, v) for slug, v in targets.items()
                    if slug in audit and audit[slug]["person"] in ("man", "woman")
                    and audit[slug]["person"] != want[v["gender"]]
                    and slug not in undo]                   # already replaced: skip
        if self.o["limit"]:
            todo = todo[: self.o["limit"]]
        self.stdout.write("to replace: %d careers" % len(todo))

        s3, bucket, base = self._spaces()
        lock = threading.Lock()
        stats = {"done": 0, "failed": 0, "attempts": 0}

        def one(slug, v):
            png, mime = None, None
            for attempt in range(3):                       # made, then checked
                with lock:
                    stats["attempts"] += 1
                img = _retry(lambda: _gemini_generate_image(
                    career_image_prompt(v["career"], v["gender"], insist=attempt > 0),
                    model=self.o["image_model"]))
                raw = base64.b64decode(img.data_b64)
                if self._looks_like(raw) == want[v["gender"]]:
                    png, mime = raw, img.mime_type or "image/png"
                    break
            if png is None:
                raise RuntimeError("3 attempts, never showed a %s" % want[v["gender"]])

            ext = mime.split("/")[-1].replace("jpeg", "jpg")
            # NEW key - the old file is never overwritten, so undo is instant
            # and production (still pointing at the old files) is unaffected.
            # The name carries a fingerprint of the bytes. Re-using a name let
            # the CDN keep serving the previous upload for up to a year (seen
            # in the pilot); a new name per image makes that impossible.
            import hashlib
            key = "career-images-v2/%s-%s.%s" % (slug, hashlib.sha1(png).hexdigest()[:10], ext)
            s3.put_object(Bucket=bucket, Key=key, Body=png, ContentType=mime,
                          ACL="public-read", CacheControl="public, max-age=31536000")
            url = "%s/%s" % (base, key)

            # Prove the link serves THIS image before the database hears of it.
            import requests
            got = requests.get(url, timeout=60)
            if got.status_code != 200 or not got.headers.get("content-type", "").startswith("image/") \
                    or len(got.content) != len(png):
                raise RuntimeError("uploaded, but %s answered %s %s (%dB of %dB) - DB not changed"
                                   % (url, got.status_code, got.headers.get("content-type"),
                                      len(got.content), len(png)))

            with lock:
                with connection.cursor() as c:
                    c.execute("select id, image_url, dg_image_url from fetch_careerjob where job_slug = %s", [slug])
                    old_rows = [{"id": i, "image_url": a or "", "dg_image_url": b or ""} for i, a, b in c.fetchall()]
                    # Old values saved BEFORE the write, and flushed to disk.
                    undo[slug] = {"career": v["career"], "gender": v["gender"], "new_url": url, "rows": old_rows}
                    UNDO.write_text(json.dumps(undo, indent=1, ensure_ascii=False))
                    c.execute("update fetch_careerjob set dg_image_url = %s, image_url = %s where job_slug = %s",
                              [url, url, slug])
                stats["done"] += 1
            return slug

        with ThreadPoolExecutor(self.o["workers"]) as ex:
            futs = {ex.submit(one, slug, v): v["career"] for slug, v in todo}
            for i, f in enumerate(as_completed(futs), 1):
                try:
                    f.result()
                except Exception as e:
                    stats["failed"] += 1
                    self.stderr.write("  FAILED %s: %s" % (futs[f], str(e)[:200]))
                if i % 25 == 0:
                    self.stdout.write("  %d/%d  (done %d, failed %d, image calls %d)"
                                      % (i, len(todo), stats["done"], stats["failed"], stats["attempts"]))
        self.stdout.write(self.style.SUCCESS(
            "replaced %d careers, failed %d, image calls %d. Undo data: %s"
            % (stats["done"], stats["failed"], stats["attempts"], UNDO)))

    # -- unify ----------------------------------------------------------------
    def _unify(self):
        """
        Careers that were already right were never replaced - but only their
        lowest-id row was checked. Their other rows kept their own old images
        (unchecked, some empty), and the app can show any row depending on a
        user's categories. Point every row at the checked, correct image.
        No generation; old values go to undo.json first.
        """
        self._guard_backup_db()
        targets = json.loads(TARGETS.read_text())
        audit = json.loads(AUDIT.read_text())
        undo = json.loads(UNDO.read_text()) if UNDO.exists() else {}
        want = {"female": "woman", "male": "man"}
        changed_careers = changed_rows = 0
        for slug, v in targets.items():
            if slug in undo:
                continue                                  # already one image on all rows
            a = audit.get(slug)
            if not a or a["person"] != want[v["gender"]] or not a.get("image"):
                continue                                  # only a CHECKED, correct image is copied
            good = a["image"]
            with connection.cursor() as c:
                c.execute("select id, image_url, dg_image_url from fetch_careerjob where job_slug = %s", [slug])
                rows = [{"id": i, "image_url": x or "", "dg_image_url": y or ""} for i, x, y in c.fetchall()]
                if all(r["dg_image_url"] == good and r["image_url"] == good for r in rows):
                    continue
                undo[slug] = {"career": v["career"], "gender": v["gender"], "new_url": good,
                              "rows": rows, "kind": "unify"}
                UNDO.write_text(json.dumps(undo, indent=1, ensure_ascii=False))
                c.execute("update fetch_careerjob set dg_image_url = %s, image_url = %s where job_slug = %s",
                          [good, good, slug])
                changed_careers += 1
                changed_rows += sum(1 for r in rows if r["dg_image_url"] != good or r["image_url"] != good)
        self.stdout.write(self.style.SUCCESS("unified %d careers (%d rows changed)" % (changed_careers, changed_rows)))

    # -- undo -----------------------------------------------------------------
    def _undo(self):
        self._guard_backup_db()
        if not UNDO.exists():
            raise CommandError("nothing to undo")
        undo = json.loads(UNDO.read_text())
        n = 0
        with connection.cursor() as c:
            for slug, u in undo.items():
                for r in u["rows"]:
                    # Only put a row back if it still holds OUR new url - never
                    # clobber something changed after us.
                    c.execute("update fetch_careerjob set image_url = %s, dg_image_url = %s "
                              "where id = %s and dg_image_url = %s",
                              [r["image_url"], r["dg_image_url"], r["id"], u["new_url"]])
                    n += c.rowcount
        UNDO.rename(UNDO.with_suffix(".undone.json"))
        self.stdout.write(self.style.WARNING("restored %d rows (new files left in Spaces)" % n))

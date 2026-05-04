# from __future__ import annotations

# import json
# import uuid
# from pathlib import Path

# from django.conf import settings
# from django.core.management.base import BaseCommand
# from django.utils import timezone
# from django.db import transaction

# from apprenticeship.models import ApprenticeshipVacancy, ApprenticeshipScrapeLog
# from apprenticeship.scrapper.gmfj_client import GmfjApprenticeshipClient


# def _categories_json_path() -> Path:
#     return Path(settings.BASE_DIR) / "apprenticeship" / "categories" / "categories.json"


# def _load_categories(path: Path):
#     return json.loads(path.read_text(encoding="utf-8"))


# class Command(BaseCommand):
#     help = "Scrape GMFJ apprenticeships using categories.json"

#     def handle(self, *args, **opts):
#         client = GmfjApprenticeshipClient()
#         categories = _load_categories(_categories_json_path())

#         run_id = uuid.uuid4()

#         created = 0
#         updated = 0

#         seen = set()

#         for category, subcats in categories.items():
#             self.stdout.write(f"\nCATEGORY: {category}")

#             for sub in subcats:
#                 self.stdout.write(f"  → Subcategory: {sub}")

#                 for listed in client.iter_all_vacancies(sub):

#                     if listed.vacancy_ref in seen:
#                         continue
#                     seen.add(listed.vacancy_ref)

#                     try:
#                         details = client.scrape_vacancy_detail(listed.url)

#                         status, _ = self._upsert(
#                             listed=listed,
#                             details=details,
#                             category=category,
#                             subcategory=sub,
#                             run_id=run_id
#                         )

#                         if status == "created":
#                             created += 1
#                         elif status == "updated":
#                             updated += 1

#                     except Exception as e:
#                         ApprenticeshipScrapeLog.objects.create(
#                             run_id=run_id,
#                             category=category,
#                             keyword=sub,
#                             vacancy_ref=listed.vacancy_ref,
#                             status="error",
#                             message=str(e)
#                         )

#         self.stdout.write(
#             self.style.SUCCESS(f"Done. created={created}, updated={updated}")
#         )

#     @transaction.atomic
#     def _upsert(self, *, listed, details, category, subcategory, run_id):

#         obj, created = ApprenticeshipVacancy.objects.get_or_create(
#             vacancy_ref=listed.vacancy_ref,
#             defaults={
#                 "vacancy_url": listed.url,
#                 "title": details.get("title") or listed.title,
#                 "employer_name": details.get("employer_name") or listed.employer_name,
#                 "location_summary": details.get("location_summary") or listed.location_summary,
#                 "summary_text": details.get("summary_text"),
#                 "wage": details.get("wage"),

#                 "category": category,
#                 "subcategory": subcategory,

#                 "last_scrape_run_id": run_id,
#                 "last_checked_at": timezone.now(),
#             }
#         )

#         if created:
#             return "created", ""

#         changed = []

#         for field in ["title", "employer_name", "location_summary", "summary_text", "wage"]:
#             new_val = details.get(field) or getattr(listed, field, "")
#             if getattr(obj, field) != new_val:
#                 setattr(obj, field, new_val)
#                 changed.append(field)

#         if changed:
#             obj.category = category
#             obj.subcategory = subcategory
#             obj.last_scrape_run_id = run_id
#             obj.last_checked_at = timezone.now()
#             obj.save()

#             return "updated", ""

#         return "skipped", ""






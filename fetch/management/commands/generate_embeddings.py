from django.core.management.base import BaseCommand
from django.db import transaction

from fetch.models import CareerJob, CareerEmbedding
from fetch.services.text_builder import build_career_text
from fetch.services.embeddings import embed_texts


class Command(BaseCommand):
    help = "Generate and store embeddings for CareerJob records"

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=32,
            help="Number of careers to embed per batch",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Optional limit on number of careers to process",
        )
        parser.add_argument(
            "--only-missing",
            action="store_true",
            help="Only generate embeddings for careers that do not already have one",
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]
        limit = options["limit"]
        only_missing = options["only_missing"]

        queryset = CareerJob.objects.all().order_by("id")

        if only_missing:
            queryset = queryset.filter(embedding_record__isnull=True)

        if limit:
            queryset = queryset[:limit]

        careers = list(queryset)

        total = len(careers)

        if total == 0:
            self.stdout.write(self.style.WARNING("No careers found to process."))
            return

        self.stdout.write(self.style.NOTICE(f"Found {total} careers to process."))

        processed = 0
        created_count = 0
        updated_count = 0
        skipped_count = 0

        for start in range(0, total, batch_size):
            batch = careers[start:start + batch_size]

            batch_careers = []
            batch_texts = []

            for career in batch:
                text = build_career_text(career).strip()

                if not text:
                    skipped_count += 1
                    self.stdout.write(
                        self.style.WARNING(
                            f"Skipping career id={career.id} because built text is empty."
                        )
                    )
                    continue

                batch_careers.append(career)
                batch_texts.append(text)

            if not batch_careers:
                continue

            vectors = embed_texts(batch_texts)

            with transaction.atomic():
                for career, text, vector in zip(batch_careers, batch_texts, vectors):
                    obj, created = CareerEmbedding.objects.update_or_create(
                        career=career,
                        defaults={
                            "embedding": vector,
                            "source_text": text,
                            "model_name": "all-MiniLM-L6-v2",
                        },
                    )

                    if created:
                        created_count += 1
                    else:
                        updated_count += 1

                    processed += 1

            self.stdout.write(
                self.style.SUCCESS(
                    f"Processed batch {start + 1}-{min(start + batch_size, total)} / {total}"
                )
            )

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Embedding generation complete."))
        self.stdout.write(f"Processed: {processed}")
        self.stdout.write(f"Created:   {created_count}")
        self.stdout.write(f"Updated:   {updated_count}")
        self.stdout.write(f"Skipped:   {skipped_count}")
# careers/serializers.py
from rest_framework import serializers
from fetch.models import CareerJob


class CareerJobSerializer(serializers.ModelSerializer):
    # optional: show human-readable career type too
    career_type_display = serializers.CharField(source="get_career_type_display", read_only=True)

    class Meta:
        model = CareerJob
        fields = [
            "id",
            "career_type",
            "career_type_display",
            "sub_type",
            "job_slug",
            "job_url",
            "jobname",
            "job_description",
            "salary",
            "hours",
            "timings",
            "how_to_become",
            "college",
            "college_entry_req",
            "apprenticeship_entry_req",
            "apprenticeship",
            "scraped_at",
        ]
        read_only_fields = ["id", "scraped_at", "career_type_display"]

    def validate(self, attrs):
        # basic safety checks (optional)
        career_type = attrs.get("career_type", getattr(self.instance, "career_type", None))
        sub_type = attrs.get("sub_type", getattr(self.instance, "sub_type", None))
        job_slug = attrs.get("job_slug", getattr(self.instance, "job_slug", None))

        if career_type and career_type not in dict(CareerJob.CareerType.choices):
            raise serializers.ValidationError({"career_type": "Invalid career_type."})

        if not sub_type:
            raise serializers.ValidationError({"sub_type": "sub_type is required."})

        if not job_slug:
            raise serializers.ValidationError({"job_slug": "job_slug is required."})

        return attrs

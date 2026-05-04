def clean_text(text: str, max_chars: int = 280) -> str:
    if not text:
        return ""

    text = text.replace("\n", " ").strip()

    # normalize bullets
    text = text.replace(" - ", ", ")
    text = text.replace("- ", "")

    # fix punctuation issues
    text = text.replace(":,", ":")
    text = text.replace(", ,", ",")
    text = text.replace(" ,", ",")

    # remove noisy repeated phrases
    text = text.replace("Entry requirements for these courses vary.", "")
    text = text.replace("Entry requirements", "")

    # normalize spacing
    text = " ".join(text.split())

    return text[:max_chars]

def build_career_text(career) -> str:
    """
    Convert CareerJob into structured text for embedding.
    Focus on semantic meaning, not raw data dumping.
    """

    parts = []

    # 🔹 Title (STRONGEST SIGNAL)
    if career.jobname:
        parts.append(f"Title: {career.jobname}")

    # 🔹 Category / Context
    if career.career_type or career.sub_type:
        parts.append(f"Category: {career.career_type} - {career.sub_type}")

    # 🔹 Description
    if career.job_description:
        parts.append(f"Description: {clean_text(career.job_description)}")

    # 🔹 How to become (VERY IMPORTANT)
    if career.how_to_become:
        how_text = clean_text(career.how_to_become)

        if "College" in how_text:
            how_text = how_text.split("College")[0]

        parts.append(f"How to Become: {how_text}")

    education_parts = []

    if career.college:
        college_text = clean_text(career.college, max_chars=200)
        education_parts.append(f"College: {college_text}")

    if career.college_entry_req:
        req_text = clean_text(career.college_entry_req, max_chars=150)
        if req_text:
            req_text = req_text.lstrip(", ")
            education_parts.append(f"Requirements: {req_text}")

    if education_parts:
        parts.append("Education Path:\n" + "\n".join(education_parts))

    # 🔹 Apprenticeship (optional but useful)
    apprenticeship_parts = []

    if career.apprenticeship:
        app_text = clean_text(career.apprenticeship, max_chars=180)

        # cut at sentence boundary if possible
        if "." in app_text:
            app_text = app_text.split(".")[0]

        apprenticeship_parts.append(f"Apprenticeship: {app_text}")

    if career.apprenticeship_entry_req:
        req = clean_text(career.apprenticeship_entry_req, max_chars=120)
        req = req.lstrip(", ")

        apprenticeship_parts.append(f"Requirements: {req}")

    if apprenticeship_parts:
        parts.append("Apprenticeship Path:\n" + "\n".join(apprenticeship_parts))

    # 🔹 Work conditions (weak signal but still useful)
    work_parts = []

    if career.salary:
        work_parts.append(f"Salary: {career.salary}")

    if career.hours:
        work_parts.append(f"Hours: {career.hours}")

    if career.timings:
        work_parts.append(f"Timings: {career.timings}")

    if work_parts:
        parts.append("Work Details:\n" + "\n".join(work_parts))

    return "\n\n".join(parts)
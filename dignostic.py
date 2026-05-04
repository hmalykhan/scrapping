"""
debug_probe.py

Two things:
1. Probe GraphQL field by field using ONLY the confirmed working fields
   (no 'description', no 'website' - those don't exist)
2. Extract and print every non-empty field from the search Apollo state
   for the Leonardo UK opportunity (id=496290)

Run: python debug_probe.py
"""

import json
import time
import cloudscraper
from bs4 import BeautifulSoup

PROXY = {
    "http":  "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
    "https": "http://inafazyf-11:4bsu6huks9c2@p.webshare.io:80/",
}

BASE_URL = "https://uk.prosple.com"
GID = "123"
ENDPOINT = "https://prosple-gw.global.ssl.fastly.net/internal"
SEARCH_URL = "https://uk.prosple.com/search-jobs?location=United+Kingdom&defaults_applied=1&keywords=software+engineer"

TEST_OPP_ID = "496290"
TEST_GROUP_ID = "47478056"


def make_scraper():
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    s.proxies = PROXY
    return s


def gql(scraper, query, variables=None):
    try:
        r = scraper.post(
            ENDPOINT,
            json={"query": query, "variables": variables or {}},
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Origin": BASE_URL,
                "Referer": BASE_URL + "/",
            },
            timeout=30,
        )
        data = r.json()
        if r.status_code == 400:
            errs = data.get("errors", [])
            msgs = [e.get("message", "") for e in errs]
            return None, msgs
        return data.get("data"), []
    except Exception as e:
        return None, [str(e)]


def main():
    scraper = make_scraper()
    vars_ = {"id": TEST_OPP_ID, "gid": GID}

    # ── PART A: GraphQL field probe ───────────────────────────────────
    print("="*60)
    print("PART A: GraphQL field probe (confirmed working endpoint)")
    print("="*60)

    # Confirmed working from minimal query
    BASE_FIELDS = "id title"

    # Fields to probe — all taken from the real Apollo object keys
    # plus common alternative names for description/body
    PROBE = [
        # Content - try every possible name for the job description
        "body",
        "content",
        "jobDescription",
        "roleDescription",
        "programDescription",
        "opportunityDescription",
        "fullDescription",
        "richDescription",
        "intro",
        "aboutRole",
        "aboutOpportunity",
        "roleInfo",
        "overview { summary }",
        "overview { summary body }",
        "overview { summary content }",
        "overview { summary description }",
        "overview { summary intro }",
        "overview { summary text }",
        "overview { summary html }",
        "overview { summary richText }",

        # Fields confirmed in Apollo state object - try them all
        "applyByUrl",
        "expired",
        "locationDescription",
        "applicationsOpenDate",
        "applicationsCloseDate",
        "applicationsCloseDateDescription",
        "workMode",
        "remoteAvailable",
        "minSalary",
        "maxSalary",
        "hideSalary",
        "salaryDescription",
        "minNumberVacancies",
        "maxNumberVacancies",
        "acceptsPreRegisters",
        "applicationsOpen",
        "experienceRequired",
        "publishedDate",
        "numberOfVacancies",

        # Structured fields from Apollo
        "salary { type value rate range { minimum maximum } currency { label } }",
        "salaryCurrency { label }",
        "startDate { ... on OpportunityStartDateExact { exactDate } ... on OpportunityStartDateCategory { category { label } } }",
        "degreeTypes { label }",
        "studyFields { id label children { id label } }",
        "opportunityTypes { id label }",
        "geoAddresses { locality streetAddress postalCode region country coordinates { lat lon } }",
        "additionalBenefits",
        "minimumGrades",
        "applicationProcess",

        # Training - try all variants
        "trainingInformation",
        "trainingInfo",
        "trainingDetails",
        "trainingProvider { name }",
        "trainingProvider { name title }",
        "trainingSchedule { label value }",
        "duration { label value }",
        "hoursPerWeek { label value }",
        "workingHours",
        "workingHoursDescription",

        # Requirements
        "requirementsSummary",
        "skills { label name }",
        "hiringCriteria { label value }",
        "criteria { label }",

        # Benefits/after
        "benefits { label }",
        "afterOpportunity",
        "futureProspects",
        "careerProspects",

        # Employer
        "parentEmployer { id title advertiserName websiteUrl }",
        "parentEmployer { id title advertiserName websiteUrl logo { thumbnail { url } } }",
        "parentEmployer { id title advertiserName websiteUrl logo { thumbnail { url } } overview { summary } }",
        "parentEmployer { id title advertiserName websiteUrl logo { thumbnail { url } } overview { summary } industrySectors { label } numberOfEmployees }",

        # Contact
        "contactPerson { name fullName }",
        "contactPerson { name fullName email }",
    ]

    valid_fields = []
    print(f"\nProbing {len(PROBE)} fields...\n")

    for field in PROBE:
        fname = field.split("{")[0].strip()
        q = f"query Q($id: ID!, $gid: ID!) {{ opportunity(id: $id, gid: $gid) {{ {BASE_FIELDS} {field} }} }}"
        data, errors = gql(scraper, q, vars_)
        if not errors and data and data.get("opportunity"):
            opp = data["opportunity"]
            val = opp.get(fname)
            if val is not None and val != "" and val != [] and val != {}:
                print(f"  ✓ VALID+DATA: {field[:70]}")
                print(f"            → {str(val)[:100].replace(chr(10),' ')}")
            else:
                print(f"  ✓ valid(empty): {fname}")
            valid_fields.append(field)
        else:
            msg = (errors[0] if errors else "?")[:80]
            if "Cannot query field" in msg:
                # Extract the suggestion if any
                suggestion = ""
                if "Did you mean" in msg:
                    suggestion = " → " + msg.split("Did you mean")[-1][:40]
                print(f"  ✗ {fname}{suggestion}")
            else:
                print(f"  ? {fname}: {msg[:60]}")
        time.sleep(0.15)

    # ── PART B: Full working query ────────────────────────────────────
    print("\n" + "="*60)
    print("PART B: Full query with all valid fields")
    print("="*60)

    if valid_fields:
        all_fields = BASE_FIELDS + " " + " ".join(valid_fields)
        q = f"query Q($id: ID!, $gid: ID!) {{ opportunity(id: $id, gid: $gid) {{ {all_fields} }} }}"
        data, errors = gql(scraper, q, vars_)
        if errors:
            print(f"  Errors building full query: {errors[:2]}")
            # Try them one by one to find conflicts
        elif data:
            opp = data.get("opportunity", {})
            print(f"\n  Data for '{opp.get('title', '')}':")
            for k, v in opp.items():
                if v is not None and v != "" and v != [] and v != {}:
                    print(f"    {k}: {str(v)[:150].replace(chr(10),' ')}")
            with open("working_gql_result.json", "w") as f:
                json.dump(opp, f, indent=2, default=str)
            print("\n  Saved to working_gql_result.json")

    # ── PART C: Print ALL non-empty Apollo state fields ───────────────
    print("\n" + "="*60)
    print("PART C: Search page Apollo state — ALL non-empty fields")
    print("        (for opportunity: Software Engineer Graduate id=496290)")
    print("="*60)

    r = scraper.get(SEARCH_URL, timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag:
        nd = json.loads(tag.string)
        apollo = nd.get("props", {}).get("pageProps", {}).get("initialApolloState", {})
        opp_key = f"Opportunity:{TEST_OPP_ID}"
        opp = apollo.get(opp_key, {})
        if opp:
            print(f"\n  Non-empty fields in {opp_key}:")
            for k, v in opp.items():
                if v is None or v == "" or v == [] or v == {}:
                    continue
                print(f"\n  [{k}]")
                print(f"    {json.dumps(v, default=str)[:300]}")
        else:
            print(f"  Key {opp_key} not found. Available Opportunity keys:")
            for k in apollo:
                if k.startswith("Opportunity:"):
                    print(f"    {k}")

        # Also print parentEmployer for this opportunity
        emp_ref = None
        if opp:
            emp_field = opp.get("parentEmployer")
            if isinstance(emp_field, dict) and "__ref" in emp_field:
                emp_ref = emp_field["__ref"]
        if emp_ref:
            emp = apollo.get(emp_ref, {})
            print(f"\n  Non-empty fields in {emp_ref} (parentEmployer):")
            for k, v in emp.items():
                if v is None or v == "" or v == [] or v == {}:
                    continue
                print(f"\n  [{k}]")
                print(f"    {json.dumps(v, default=str)[:300]}")

        with open("apollo_opp_full.json", "w") as f:
            json.dump(opp, f, indent=2, default=str)
        print("\n  Saved opportunity Apollo object to apollo_opp_full.json")

    scraper.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
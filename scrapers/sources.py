"""
Source registry — the single place that records, per website, whether it has
already been scraped and must NOT be touched again.

This is a plain Python config (NOT a database table) on purpose:
  - no migration, no schema change, nothing that can affect existing data.

Status meaning
--------------
DONE   : fully scraped already. The engine REFUSES to run it (use --force to override).
PAUSED : partially scraped. The engine runs it ONLY in incremental mode, so items
         already in the DB are skipped and finished work is never redone
         (use --resume to allow; a normal run still refuses).
TODO   : not started yet. These are the engine's primary targets.

`vertical` decides which existing table a site's data lands in:
  job | course | apprenticeship | career
"""

DONE = "done"
PAUSED = "paused"
TODO = "todo"

SOURCES = {
    # ---------------------------------------------------------------
    # FULLY SCRAPED — frozen. Never scraped again (engine refuses).
    # ---------------------------------------------------------------
    "ncs_careers": {
        "name": "National Careers Service — Explore Careers",
        "base_url": "https://nationalcareers.service.gov.uk",
        "vertical": "career",
        "status": DONE,
    },
    "ncs_courses": {
        "name": "National Careers Service — Find a Course",
        "base_url": "https://nationalcareers.service.gov.uk/find-a-course",
        "vertical": "course",
        "status": DONE,
    },
    "ncs_apprenticeships": {
        "name": "Find an Apprenticeship Service",
        "base_url": "https://www.findapprenticeship.service.gov.uk",
        "vertical": "apprenticeship",
        "status": DONE,
    },

    # ---------------------------------------------------------------
    # PARTIALLY SCRAPED — paused. Resume only (incremental skip).
    # ---------------------------------------------------------------
    "dwp_jobs": {
        "name": "DWP Find a Job",
        "base_url": "https://findajob.dwp.gov.uk",
        "vertical": "job",
        "status": PAUSED,
    },
    "prosple": {
        "name": "Prosple UK",
        "base_url": "https://uk.prosple.com",
        "vertical": "job",
        "status": PAUSED,
    },
    "gmfj": {
        "name": "GetMyFirstJob",
        "base_url": "https://www.getmyfirstjob.co.uk",
        "vertical": "job",
        "status": PAUSED,
    },
    "kiwi": {
        "name": "Kiwi Education",
        "base_url": "https://kiwieducation.co.uk",
        "vertical": "course",
        "status": PAUSED,
    },
    "ucas": {
        "name": "UCAS (courses + apprenticeships)",
        "base_url": "https://www.ucas.com",
        "vertical": "course",
        "status": PAUSED,
    },

    # ---------------------------------------------------------------
    # NOT STARTED — todo. The new engine's job.
    # ---------------------------------------------------------------
    # Apprenticeships
    "apprenticeships_gov": {
        "name": "Apprenticeships.gov.uk",
        "base_url": "https://www.apprenticeships.gov.uk",
        "vertical": "apprenticeship",
        "status": TODO,
    },
    "find_appr_training": {
        "name": "Apprenticeship Training Courses",
        "base_url": "https://findapprenticeshiptraining.apprenticeships.education.gov.uk",
        "vertical": "apprenticeship",
        "status": TODO,
    },
    # Graduate jobs
    "prospects_grad": {
        "name": "Prospects Graduate Jobs",
        "base_url": "https://www.prospects.ac.uk/graduate-jobs",
        "vertical": "job",
        "status": TODO,
    },
    "targetjobs": {
        "name": "TargetJobs",
        "base_url": "https://targetjobs.co.uk",
        "vertical": "job",
        "status": TODO,
    },
    "brightnetwork": {
        "name": "Bright Network",
        "base_url": "https://www.brightnetwork.co.uk",
        "vertical": "job",
        "status": TODO,
    },
    "gradcracker": {
        "name": "Gradcracker",
        "base_url": "https://www.gradcracker.com",
        "vertical": "job",
        "status": TODO,
    },
    "milkround": {
        "name": "Milkround",
        "base_url": "https://www.milkround.com",
        "vertical": "job",
        "status": TODO,
    },
    # University / courses
    "discoveruni": {
        "name": "Discover Uni",
        "base_url": "https://discoveruni.gov.uk",
        "vertical": "course",
        "status": TODO,
    },
    "prospects_pg": {
        "name": "Prospects Postgraduate Courses",
        "base_url": "https://www.prospects.ac.uk/postgraduate-study",
        "vertical": "course",
        "status": TODO,
    },
    "theuniguide": {
        "name": "The Uni Guide",
        "base_url": "https://www.theuniguide.co.uk",
        "vertical": "course",
        "status": TODO,
    },
    # Career information
    "careerpilot": {
        "name": "Careerpilot",
        "base_url": "https://www.careerpilot.org.uk",
        "vertical": "career",
        "status": TODO,
    },
    "prospects_profiles": {
        "name": "Prospects Job Profiles",
        "base_url": "https://www.prospects.ac.uk/job-profiles",
        "vertical": "career",
        "status": TODO,
    },
    "icould": {
        "name": "iCould",
        "base_url": "https://icould.com",
        "vertical": "career",
        "status": TODO,
    },
    "skillsforcareers": {
        "name": "Skills for Careers",
        "base_url": "https://www.skillsforcareers.education.gov.uk",
        "vertical": "career",
        "status": TODO,
    },
    "getintoteaching": {
        "name": "Get Into Teaching",
        "base_url": "https://getintoteaching.education.gov.uk",
        "vertical": "career",
        "status": TODO,
    },
    # Work experience / internships
    "futuresforall_finder": {
        "name": "Futures For All — Work Experience Finder",
        "base_url": "https://finder.futuresforall.org",
        "vertical": "job",
        "status": TODO,
    },
    "myglobalbridge": {
        "name": "MyGlobalBridge",
        "base_url": "https://www.myglobalbridge.com",
        "vertical": "job",
        "status": TODO,
    },
    "springpod": {
        "name": "Springpod",
        "base_url": "https://www.springpod.com",
        "vertical": "job",
        "status": TODO,
    },
    "speakersforschools": {
        "name": "Speakers for Schools",
        "base_url": "https://www.speakersforschools.org",
        "vertical": "job",
        "status": TODO,
    },
    "uptree": {
        "name": "Uptree",
        "base_url": "https://uptree.co",
        "vertical": "job",
        "status": TODO,
    },
    "youngprofessionals": {
        "name": "Young Professionals",
        "base_url": "https://www.young-professionals.uk",
        "vertical": "job",
        "status": TODO,
    },
    "ratemyplacement": {
        "name": "RateMyPlacement",
        "base_url": "https://www.ratemyplacement.co.uk",
        "vertical": "job",
        "status": TODO,
    },
    "forage": {
        "name": "Forage Virtual Work Experience",
        "base_url": "https://www.theforage.com",
        "vertical": "job",
        "status": TODO,
    },
}


def get_source(key: str) -> dict:
    if key not in SOURCES:
        raise KeyError(
            f"Unknown source '{key}'. Known sources: {', '.join(sorted(SOURCES))}"
        )
    return {**SOURCES[key], "key": key}

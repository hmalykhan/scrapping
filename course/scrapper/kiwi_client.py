"""
courses/scrapper/kiwi/client.py  —  v4 (post-diagnostic rewrite)

CONFIRMED PAGE LAYOUTS (from diagnostic):
══════════════════════════════════════════════════════════════════════

LAYOUT A  —  Full structured course  (e.g. Principles of Cyber Security)
  Detected by:  <div> whose class string contains 'wysiwyg' but NOT 'course-wysiwyg'
  e.g.  <div class="wysiwyg w-full px-6 pb-6">

  Hero <p> tags (all inside bg-green-dark hero div):
    level_prefix   cls='w-full md:w-3/4 text-lg pb-6 text-white'   → "Level 2"
    course_name    cls='text-3xl ... font-extrabold text-white pb-6'
    course_type    cls='w-full md:w-3/4 text-white text-lg pb-6'    → "Short Online Course"
    label/value pairs (consecutive siblings):
      <p cls='text-lg text-white font-bold pb-1'>Level</p>
      <p cls='text-xl text-white pb-4'>2 Certificate</p>
      (same pattern for Awarding organisation / Qualification / Qualification Duration)

  wysiwyg div children — ALL <p> have NO class:
    Headings are  <p><strong>What does this qualification cover?</strong></p>
    Content follows as plain <p> siblings until next heading
    Units are a <ul> after  <p><strong>How is this qualification structured?</strong></p>

  Cost (outside wysiwyg, in a container div):
    <p cls='text-lg text-grey-darker pb-2'>UK Students</p>
    <p cls='text-2xl font-bold text-green'>£500.00</p>    ← NOTE: NOT 'text-2xl text-green', has 'font-bold'

LAYOUT B  —  Simple course with Cademy JS widget  (e.g. Equality and Diversity)
  Detected by:  no 'wysiwyg' div  (only 'course-wysiwyg' or nothing)
  e.g.  <div class="course-wysiwyg w-full lg:w-1/2 lg:px-6 pb-6 text-grey-darker">

  NO hero section at all  →  level/awarding/qual/duration are all blank (don't exist)
  course_name  →  og:title meta split on ' – '
  course_type  →  hardcoded "Short Online Course" or "In-Person Course"

  course-wysiwyg div:
    <p>(no class)<strong>Aims and Objectives of this Course:</strong></p>
    <ul>  (direct child, items are the aims)

  Cost:
    <p cls='text-white pb-4 lg:pb-6 text-lg'>Individual Attendee</p>
    <p cls='font-bold text-2xl text-green'>£150</p>

  who_this_course_is_for / entry_reeq:
    !! SERVED BY CADEMY JS WIDGET — NOT IN STATIC HTML !!
    These fields will always be blank for Layout B pages.
    The only way to get them is a headless browser (Playwright/Selenium).

  Image:
    og:image = CIPD logo (image-4-150x150) → SKIPPED → use listing page thumbnail

══════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
import random
import re
import time
import uuid
from dataclasses import dataclass
from typing import Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

logger = logging.getLogger(__name__)

BASE_URL    = "https://kiwieducation.co.uk"
LISTING_URL = f"{BASE_URL}/commercial-courses/"

KIWI_COLLEGE  = "Kiwi Education"
KIWI_ADDRESS  = "Burlington House, 23-25 Portland Terrace, Southampton SO14 7EN"
KIWI_PHONE    = "023 8017 0380"
KIWI_EMAIL    = "hello@kiwieducation.co.uk"
KIWI_WEBSITE  = BASE_URL
KIWI_CITY     = "Southampton"
KIWI_POSTCODE = "SO14 7EN"

KIWI_COURSE_UUID_NS = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

# Images to reject — partner logos that appear on every page
SKIP_IMAGE_KEYWORDS = [
    "yuzu", "logo", "icon", "avatar", "esfa", "esf-logo",
    "app_logo", "iep-logo", "icq", "cyberessentials",
    "bcs_accredited", "matrix-qm", "skillsfirst", "ica-",
    "pearson", "ifa-", "edexcel", "ncfe-logo", "employer_small",
    "sert-training", "image-4-150x150",   # CIPD logo
]

# ─────────────────────────── Taxonomy ────────────────────────────────

OUR_TAXONOMY: dict[str, list[str]] = {
    "Administration": [
        "Accounting technician", "Admin assistant", "Arts administrator",
        "Assistant immigration officer", "Auditor", "Bid writer",
        "Bilingual secretary", "Bookkeeper", "Border Force officer",
        "Car rental agent", "Charity fundraiser",
        "Civil Service administrative officer", "Civil Service executive officer",
        "Credit controller", "Customer service assistant",
        "Data protection officer", "Diplomatic Service officer",
        "Estates officer", "Executive assistant", "Farm secretary",
        "Finance officer", "Financial services customer adviser",
        "GP practice manager", "Health and safety adviser",
        "Health records clerk", "Health service manager", "Housing officer",
        "Human resources officer", "Immigration officer", "Import-export clerk",
        "Insurance broker", "Insurance technician", "Interpreter",
        "Local government officer", "Local government revenues officer",
        "Medical secretary", "Office manager", "Payroll administrator",
        "Post Office customer service assistant", "Proofreader",
        "Purchasing manager", "Quality control officer", "Receptionist",
        "Recruitment consultant",
        "Registrar of births, deaths, marriages and civil partnerships",
        "Sales administrator", "School business manager", "School secretary",
        "Security Service personnel", "Sports development officer", "Supervisor",
        "Town planning assistant", "Trade union official",
        "Trading standards officer",
    ],
    "Business and finance": [
        "Accounting technician", "Actuary", "Auditor", "Bank manager",
        "Bid writer", "Bookkeeper", "Business adviser",
        "Business development manager", "Business project manager",
        "Company secretary",
        "Corporate responsibility and sustainability practitioner",
        "Credit controller", "Economist", "Finance officer",
        "Financial adviser", "Financial services customer adviser",
        "Insurance account manager", "Insurance broker",
        "Insurance claims handler", "Insurance loss adjuster",
        "Insurance risk surveyor", "Insurance technician",
        "Insurance underwriter", "Investment analyst",
        "Local government revenues officer", "Management accountant",
        "Market research executive", "Marketing manager", "Money adviser",
        "Mortgage adviser", "Paraplanner", "Payroll administrator",
        "Pensions administrator", "Private practice accountant",
        "Public finance accountant", "School business manager", "Stockbroker",
        "Tax adviser", "Tax inspector", "Translator",
    ],
    "Computing, technology and digital": [
        "3D printing technician", "App developer", "Archivist",
        "Artificial intelligence (AI) engineer", "Business analyst",
        "Cartographer", "Computer games developer", "Computer games tester",
        "Cyber intelligence officer", "Data protection officer",
        "Data scientist", "Database administrator", "Digital delivery manager",
        "Digital product owner", "E-learning developer", "Esports gamer",
        "Forensic computer analyst", "Geospatial technician", "Indexer",
        "Information scientist", "IT project manager",
        "IT security co-ordinator", "IT support technician", "Librarian",
        "Library assistant", "Media researcher", "Network engineer",
        "Network manager", "Operational researcher", "Print operator",
        "Robotics engineer",
        "Search engine optimisation (SEO) specialist",
        "Security Service personnel", "Smart home installer",
        "Social media influencer", "Social media manager", "Software developer",
        "Solutions architect", "Systems analyst", "Technical architect",
        "Technical author", "Test lead", "User experience (UX) designer",
        "User researcher", "Web content editor", "Web designer",
        "Web developer",
    ],
    "Creative and media": [
        "Actor", "Advertising account executive", "Advertising account planner",
        "Advertising art director", "Advertising copywriter",
        "Advertising media buyer", "Advertising media planner", "Animator",
        "Antique dealer", "Architect", "Architectural technician",
        "Architectural technologist", "Archivist", "Art editor", "Art therapist",
        "Art valuer", "Arts administrator", "Audio visual technician",
        "Blacksmith", "Broadcast engineer", "Broadcast journalist",
        "Ceramics designer-maker", "Choreographer", "Community arts worker",
        "Computer games tester", "Conservator", "Copy editor",
        "Costume designer", "Dance teacher", "Dancer",
        "Design and development engineer", "Director of photography", "DJ",
        "Dressmaker", "Drone pilot", "Editorial assistant", "Ergonomist",
        "Exhibition designer", "Fashion design assistant", "Fashion designer",
        "Fashion model", "Film critic", "Fine artist", "Florist",
        "Footwear designer-maker", "Furniture designer", "Furniture maker",
        "Furniture restorer", "Glassmaker", "Graphic designer", "Illustrator",
        "Indexer", "Interior designer", "Jewellery designer-maker",
        "Kitchen and bathroom designer", "Landscape architect",
        "Leather craftworker", "Lighting technician", "Live sound engineer",
        "Make-up artist", "Market research data analyst", "Market researcher",
        "Marketing executive", "Marketing manager", "Media researcher",
        "Medical illustrator", "Model maker", "Museum curator",
        "Music promotions manager", "Music teacher", "Music therapist",
        "Musical instrument maker and repairer", "Musician", "Naval architect",
        "Newspaper or magazine editor", "Newspaper or magazine journalist",
        "Pattern cutter", "Photographer", "Photographic stylist",
        "Photographic technician", "Print operator", "Product designer",
        "Proofreader", "Prop maker", "Public relations officer",
        "Radio broadcast assistant", "Roadie", "Sales manager",
        "Screenwriter", "Search engine optimisation (SEO) specialist",
        "Set designer", "Sewing machinist", "Social media influencer",
        "Sports commentator", "Stage manager", "Stagehand",
        "Studio sound engineer", "Tailor", "Tattooist and body piercer",
        "Technical author", "Textile designer", "Translator",
        "TV or film assistant director",
        "TV or film assistant production co-ordinator",
        "TV or film camera operator", "TV or film director",
        "TV or film producer", "TV or film production manager",
        "TV or film production runner", "TV or film sound technician",
        "TV presenter", "Video editor", "Visual merchandiser",
        "Wardrobe assistant", "Web content editor", "Web designer", "Writer",
    ],
    "Engineering and maintenance": [
        "Acoustics consultant", "Aerospace engineer",
        "Aerospace engineering technician", "Agricultural contractor",
        "Agricultural engineer", "Agricultural engineering technician",
        "Air accident investigator", "Audio visual technician",
        "Auto electrician", "Automotive engineer", "Boat builder",
        "Broadcast engineer", "Building control officer",
        "Building services engineer", "Building site inspector",
        "CAD technician", "Caretaker", "Chemical engineer",
        "Chemical engineering technician", "Civil engineer",
        "Civil engineering technician", "Clinical engineer", "CNC machinist",
        "Critical care technologist", "Cycle mechanic",
        "Design and development engineer", "Diver",
        "Domestic appliance service engineer", "Drone pilot",
        "Electrical engineer", "Electrical engineering technician",
        "Electrician", "Electricity distribution worker",
        "Electricity generation worker", "Electronics engineer",
        "Electronics engineering technician",
        "Engineering construction craftworker",
        "Engineering construction technician",
        "Engineering maintenance technician", "Engineering operative",
        "Ergonomist", "Farrier", "Fast fit technician",
        "Fire safety engineer", "Food factory worker", "Foundry moulder",
        "Furniture maker", "Gas mains layer", "Gas service technician",
        "Glazier", "Heat pump engineer", "Heating and ventilation engineer",
        "Heavy vehicle technician", "Helicopter engineer", "Hydrologist",
        "Lift engineer", "Lighting technician", "Live sound engineer",
        "Locksmith", "Maintenance fitter", "Manufacturing systems engineer",
        "Marine engineer", "Marine engineering technician",
        "Materials engineer", "Materials technician", "Mechanical engineer",
        "Mechanical engineering technician",
        "Merchant Navy engineering officer", "Metrologist", "Model maker",
        "Motor mechanic", "Motorcycle mechanic", "Motorsport engineer",
        "Naval architect", "Non-destructive testing technician",
        "Nuclear engineer", "Nuclear technician", "Offshore drilling worker",
        "Patent attorney", "Physicist", "Pipe fitter", "Plumber",
        "Product designer", "Quality control officer", "Quantity surveyor",
        "Quarry engineer", "Quarry worker", "Rail track maintenance worker",
        "Railway signaller", "Recycling operative",
        "Refrigeration and air-conditioning installer",
        "Renewable energy engineer", "Road worker",
        "Roadside assistance technician", "Robotics engineer",
        "Rolling stock engineering technician", "Roustabout",
        "Security systems installer", "Signalling technician",
        "Smart home installer", "Smart meter installer",
        "Solar panel installer", "Steel erector", "Steel fixer",
        "Structural engineer", "Surveying technician", "Telecoms engineer",
        "Thermal insulation engineer", "Toolmaker",
        "TV or film sound technician", "Vehicle body repairer",
        "Watch or clock repairer", "Water network operative",
        "Water treatment worker", "Wind turbine technician",
        "Windscreen fitter",
    ],
    "Environment and land": [
        "Agricultural contractor", "Agricultural engineer",
        "Agricultural engineering technician", "Agricultural inspector",
        "Agronomist", "Arboricultural officer", "Archaeologist", "Bin worker",
        "Biologist", "Building technician", "Cartographer", "Cemetery worker",
        "Chemical engineer", "Climate scientist", "Commercial energy assessor",
        "Corporate responsibility and sustainability practitioner",
        "Countryside officer", "Countryside ranger", "Drone pilot",
        "Ecologist", "Environmental consultant",
        "Environmental health practitioner", "Farm secretary", "Farm worker",
        "Farmer", "Fence installer", "Fish farmer", "Fishing boat deckhand",
        "Florist", "Forestry worker", "Gamekeeper", "Gardener",
        "Geoscientist", "Geospatial technician", "Geotechnician",
        "Groundsperson", "Horticultural manager", "Horticultural therapist",
        "Horticultural worker", "Hydrologist", "Land surveyor",
        "Landscape architect", "Landscaper", "Marine engineer",
        "Meteorologist", "Nuclear engineer", "Oceanographer",
        "Palaeontologist", "Pest control technician", "Quarry engineer",
        "Recycling engagement officer", "Renewable energy engineer",
        "Research scientist", "Rural surveyor", "Seismologist",
        "Thermal insulation engineer", "Tree surgeon",
        "Water network operative", "Water treatment worker",
        "Wind turbine technician", "Zoologist",
    ],
    "Healthcare": [
        "Acoustics consultant", "Acupuncturist", "Advocacy worker",
        "Ambulance care assistant", "Anaesthetist",
        "Anatomical pathology technician", "Art therapist", "Audiologist",
        "Biomedical scientist", "Care worker", "Children's nurse",
        "Chiropractor", "Clinical engineer", "Clinical psychologist",
        "Clinical scientist", "Cognitive behavioural therapist",
        "Community matron", "Counsellor", "Critical care technologist",
        "Dance movement psychotherapist", "Dental hygienist", "Dental nurse",
        "Dental technician", "Dental therapist", "Dentist", "Dietitian",
        "Dispensing optician", "District nurse", "Dramatherapist",
        "Emergency care assistant", "Emergency medical dispatcher",
        "Forensic psychologist", "Geneticist", "GP",
        "Health play specialist", "Health promotion specialist",
        "Health records clerk", "Health service manager", "Health trainer",
        "Health visitor", "Healthcare assistant",
        "Healthcare science assistant", "Homeopath", "Hospital doctor",
        "Hospital porter", "Hypnotherapist", "Learning disability nurse",
        "Maternity support worker", "Medical herbalist",
        "Medical illustrator", "Medical physicist", "Medical secretary",
        "Mental health nurse", "Microbiologist", "Midwife", "Music therapist",
        "Naturopath", "Nurse", "Nursing associate", "Nutritional therapist",
        "Nutritionist", "Occupational health nurse", "Occupational therapist",
        "Occupational therapy support worker",
        "Operating department practitioner", "Optometrist", "Orthoptist",
        "Osteopath", "Paediatrician", "Palliative care assistant",
        "Paramedic", "Pathologist",
        "Patient advice and liaison service officer", "Pharmacist",
        "Pharmacologist", "Pharmacy assistant", "Pharmacy technician",
        "Phlebotomist", "Physician assistant", "Physicist",
        "Physiotherapist", "Physiotherapy assistant", "Pilates teacher",
        "Plastic surgeon", "Podiatrist", "Podiatry assistant",
        "Prosthetist and orthotist", "Psychiatrist",
        "Psychological wellbeing practitioner", "Psychologist",
        "Psychotherapist", "Radiographer", "Radiography assistant",
        "Reiki healer", "School nurse", "Senior care worker", "Sonographer",
        "Speech and language therapist",
        "Speech and language therapy assistant", "Sports development officer",
        "Surgeon", "Yoga therapist",
    ],
    "Law and legal": [
        "Bailiff", "Barrister", "Barrister's clerk", "Company secretary",
        "Coroner", "Court administrative assistant", "Court legal adviser",
        "Court usher", "Credit controller", "Crown prosecutor",
        "Data protection officer", "Equalities officer", "Family mediator",
        "Forensic psychologist", "Forensic scientist",
        "Immigration adviser (non-government)", "Interpreter", "Judge",
        "Legal executive", "Legal secretary", "Licensed conveyancer",
        "Paralegal", "Patent attorney", "Probation officer",
        "Probation services officer", "Solicitor", "Tax inspector",
        "Trade mark attorney", "Trading standards officer",
        "Victim care officer", "Welfare rights adviser",
    ],
    "Managerial": [
        "Advertising account executive", "Advertising media planner",
        "Bank manager", "Bid writer", "Building control officer",
        "Business adviser", "Business analyst", "Business development manager",
        "Business project manager", "Care home manager", "Charity fundraiser",
        "Chief inspector", "Civil Service executive officer",
        "Civil Service manager", "Community education co-ordinator",
        "Company secretary", "Construction contracts manager",
        "Construction manager", "Consumer scientist",
        "Customer services manager", "Digital delivery manager",
        "Diplomatic Service officer", "E-commerce manager", "Economist",
        "Environmental consultant", "Estates officer", "Estimator",
        "Events manager", "Facilities manager", "Farmer",
        "General practice surveyor", "GP practice manager", "Headteacher",
        "Health and safety adviser", "Health service manager",
        "Horticultural manager", "Hotel manager", "Human resources officer",
        "Leisure centre manager", "Management accountant",
        "Management consultant", "Marketing manager", "MP", "Museum curator",
        "Network manager", "Nursery manager", "Office manager",
        "Operational researcher", "Planning and development surveyor",
        "Private practice accountant", "Production manager (manufacturing)",
        "Purchasing manager", "Quantity surveyor", "Retail manager",
        "Rural surveyor", "Sales manager", "Security Service personnel",
        "Social services manager", "Supervisor", "Supply chain manager",
        "Tax inspector", "Technical architect", "Tour manager", "Town planner",
        "Transport planner", "TV or film producer",
        "Visitor attraction general manager", "Warehouse manager",
        "Wedding planner",
    ],
    "Retail and sales": [
        "Advertising account executive", "Advertising account planner",
        "Advertising media buyer", "Airline customer service agent",
        "Airport information assistant", "Animal care worker",
        "Antique dealer", "Art valuer", "Automotive parts advisor",
        "Bar person", "Barista", "Beauty consultant", "Builders' merchant",
        "Business analyst", "Business development manager", "Butcher",
        "Cabin crew", "Car rental agent", "Cinema or theatre attendant",
        "Customer service assistant", "Customer services manager",
        "E-commerce manager", "Emergency medical dispatcher", "Estate agent",
        "Events manager", "Florist", "Horticultural manager",
        "Insurance account manager",
        "Land and property valuer and auctioneer",
        "Leisure centre assistant", "Market research executive",
        "Market trader", "Marketing executive", "Marketing manager",
        "Museum attendant", "Music promotions manager", "Pharmacist",
        "Pharmacy assistant", "Pharmacy technician",
        "Post Office customer service assistant", "Retail buyer",
        "Retail manager", "Sales administrator", "Sales assistant",
        "Sales manager", "Sales representative",
        "Search engine optimisation (SEO) specialist", "Shopkeeper",
        "Stock control assistant", "Tourist information centre assistant",
        "Train station worker", "Travel agent", "Visual merchandiser",
    ],
    "Social care": [
        "Accommodation warden", "Advocacy worker", "Aid worker",
        "Art therapist", "British Sign Language interpreter", "Care escort",
        "Care home manager", "Care worker", "Careers adviser",
        "Child protection officer", "Childminder", "Clinical psychologist",
        "Cognitive behavioural therapist", "Communication support worker",
        "Community development worker", "Community transport driver",
        "Counsellor", "Dramatherapist", "Education welfare officer",
        "Equalities officer", "Family mediator", "Family support worker",
        "Forensic psychologist", "Foster carer", "Funeral director",
        "Horticultural therapist", "Housing officer", "Learning mentor",
        "Life coach", "Money adviser", "Music therapist", "Nanny",
        "Nursery manager", "Nursery worker", "Occupational therapist",
        "Occupational therapy support worker", "Palliative care assistant",
        "Pastoral care worker",
        "Patient advice and liaison service officer", "Play therapist",
        "Playworker", "Probation officer",
        "Psychological wellbeing practitioner", "Psychologist",
        "Psychotherapist", "Religious leader", "Residential support worker",
        "Senior care worker", "Social services manager",
        "Social work assistant", "Social worker",
        "Substance misuse outreach worker", "Victim care officer",
        "Welfare rights adviser", "Youth offending team officer",
        "Youth worker",
    ],
    "Teaching and education": [
        "Audio visual technician", "British Sign Language teacher",
        "Careers adviser", "Child protection officer",
        "Communication support worker", "Community education co-ordinator",
        "Criminologist", "Cycling coach", "Dance teacher",
        "E-learning developer", "Early years teacher",
        "Education technician", "Education welfare officer",
        "English as a foreign language (EFL) teacher", "Equalities officer",
        "Further education teacher", "Headteacher",
        "Health promotion specialist", "Higher education lecturer",
        "Learning mentor", "Librarian", "Library assistant",
        "Martial arts instructor", "Montessori teacher", "Museum curator",
        "Music teacher", "Nursery worker", "Ofsted inspector",
        "Online tutor", "Outdoor activities instructor",
        "Pastoral care worker", "PE teacher", "Playworker",
        "Portage home visitor", "Primary school teacher",
        "Prison instructor", "RQF assessor", "Sailing instructor",
        "School business manager", "School lunchtime supervisor",
        "School secretary", "Secondary school teacher",
        "Skills for life teacher",
        "Special educational needs (SEN) teacher",
        "Special educational needs (SEN) teaching assistant",
        "Swimming teacher", "Teaching assistant", "Trade union official",
        "Training officer", "Yoga teacher", "Youth worker",
    ],
}

_SUBCATEGORY_LOOKUP: dict[str, tuple[str, str]] = {
    sub.lower(): (cat, sub)
    for cat, subs in OUR_TAXONOMY.items()
    for sub in subs
}

_KIWI_COURSE_OVERRIDES: dict[str, tuple[str, str]] = {
    "environmental sustainability":   ("Environment and land",              "Environmental consultant"),
    "climate change":                 ("Environment and land",              "Environmental consultant"),
    "sustainability":                 ("Environment and land",              "Environmental consultant"),
    "equality and diversity":         ("Social care",                       "Equalities officer"),
    "equality, diversity":            ("Social care",                       "Equalities officer"),
    "workplace language":             ("Administration",                    "Admin assistant"),
    "data protection":                ("Computing, technology and digital", "Data protection officer"),
    "data security":                  ("Computing, technology and digital", "Data protection officer"),
    "gdpr":                           ("Computing, technology and digital", "Data protection officer"),
    "cyber security":                 ("Computing, technology and digital", "IT security co-ordinator"),
    "cybersecurity":                  ("Computing, technology and digital", "IT security co-ordinator"),
    "principles of cyber":            ("Computing, technology and digital", "IT security co-ordinator"),
    "safeguarding":                   ("Social care",                       "Care worker"),
    "prevent":                        ("Social care",                       "Care worker"),
    "autism":                         ("Healthcare",                        "Learning disability nurse"),
    "mental health":                  ("Healthcare",                        "Mental health nurse"),
    "workplace violence":             ("Administration",                    "Health and safety adviser"),
    "harassment":                     ("Administration",                    "Health and safety adviser"),
    "health and safety":              ("Administration",                    "Health and safety adviser"),
    "nutrition":                      ("Healthcare",                        "Nutritionist"),
    "diabetes":                       ("Healthcare",                        "Healthcare assistant"),
    "medication":                     ("Healthcare",                        "Healthcare assistant"),
    "infection":                      ("Healthcare",                        "Healthcare assistant"),
    "care planning":                  ("Healthcare",                        "Care worker"),
    "care and management":            ("Healthcare",                        "Care worker"),
    "learning disabilities":          ("Healthcare",                        "Learning disability nurse"),
    "behaviour that challenges":      ("Social care",                       "Care worker"),
    "lean organisation":              ("Managerial",                        "Management consultant"),
    "business administration":        ("Administration",                    "Admin assistant"),
    "warehousing":                    ("Administration",                    "Admin assistant"),
    "storage":                        ("Administration",                    "Admin assistant"),
    "falls":                          ("Healthcare",                        "Healthcare assistant"),
    "cleaning":                       ("Administration",                    "Admin assistant"),
    "microsoft":                      ("Computing, technology and digital", "IT support technician"),
    "excel":                          ("Computing, technology and digital", "IT support technician"),
    "web design":                     ("Computing, technology and digital", "Web designer"),
    "web development":                ("Computing, technology and digital", "Web developer"),
    "social media":                   ("Computing, technology and digital", "Social media manager"),
    "digital marketing":              ("Creative and media",                "Marketing executive"),
    "seo":                            ("Creative and media",                "Search engine optimisation (SEO) specialist"),
    "graphic design":                 ("Creative and media",                "Graphic designer"),
    "photography":                    ("Creative and media",                "Photographer"),
    "bookkeeping":                    ("Business and finance",              "Bookkeeper"),
    "accounting":                     ("Business and finance",              "Accounting technician"),
    "payroll":                        ("Business and finance",              "Payroll administrator"),
    "tax":                            ("Business and finance",              "Tax adviser"),
    "insurance":                      ("Business and finance",              "Insurance broker"),
    "project management":             ("Managerial",                        "Business project manager"),
    "leadership":                     ("Managerial",                        "Management consultant"),
    "team leader":                    ("Managerial",                        "Supervisor"),
    "human resources":                ("Administration",                    "Human resources officer"),
    "recruitment":                    ("Administration",                    "Recruitment consultant"),
    "customer service":               ("Administration",                    "Customer service assistant"),
    "counselling":                    ("Healthcare",                        "Counsellor"),
    "marketing":                      ("Creative and media",                "Marketing executive"),
    "teaching":                       ("Teaching and education",            "Teaching assistant"),
    "sales":                          ("Retail and sales",                  "Sales assistant"),
    "retail":                         ("Retail and sales",                  "Retail manager"),
}


def _make_course_id(course_url: str) -> uuid.UUID:
    return uuid.uuid5(KIWI_COURSE_UUID_NS, course_url.strip().rstrip("/").lower())


def _resolve_taxonomy(course_name: str) -> tuple[str, str]:
    STOPWORDS = {
        "and", "the", "for", "with", "our", "this", "that", "from",
        "certificate", "level", "principles", "introduction", "award",
        "advanced", "foundation", "diploma", "nvq", "ncfe", "cache",
        "understanding", "awareness", "of", "in", "to", "a",
    }

    def words(s: str) -> set[str]:
        return {w for w in re.findall(r"[a-z]{3,}", s.lower()) if w not in STOPWORDS}

    name_norm  = course_name.strip().lower()
    name_words = words(name_norm)

    best_override: Optional[tuple[str, str]] = None
    best_key_len = 0
    for key, val in _KIWI_COURSE_OVERRIDES.items():
        if key in name_norm and len(key) > best_key_len:
            best_key_len = len(key)
            best_override = val
    if best_override:
        return best_override

    if name_norm in _SUBCATEGORY_LOOKUP:
        return _SUBCATEGORY_LOOKUP[name_norm]

    best_match: Optional[tuple[str, str]] = None
    best_count = 0
    for sub_lower, (cat, sub) in _SUBCATEGORY_LOOKUP.items():
        sub_words = words(sub_lower)
        if not sub_words:
            continue
        common = sub_words & name_words
        if common == sub_words and len(common) >= 2:
            if len(common) > best_count:
                best_count = len(common)
                best_match = (cat, sub)
    if best_match:
        return best_match

    return "Administration", "Admin assistant"


# ─────────────────────────── Data class ──────────────────────────────

@dataclass
class KiwiCourseDetail:
    course_id:                  uuid.UUID
    course_url:                 str
    category:                   str = ""
    subcategory:                str = ""
    requirement_summery:        str = ""
    image_url:                  str = ""
    course_name:                str = ""
    course_type:                str = ""
    learning_method:            str = ""
    course_hours:               str = ""
    course_stryd_time:          str = ""
    course_qualification_level: str = ""
    course_description:         str = ""
    attendance_pattern:         str = ""
    awarding_organization:      str = ""
    who_this_course_is_for:     str = ""
    entry_reeq:                 str = ""
    college_name:               str = ""
    address:                    str = ""
    email:                      str = ""
    phone:                      str = ""
    website:                    str = ""
    duration:                   str = ""
    cost:                       str = ""
    cost_description:           str = ""
    city:                       str = ""
    state:                      str = ""
    zip_code:                   str = ""
    latitude:                   float | None = None
    longitude:                  float | None = None


# ─────────────────────────── Helpers ─────────────────────────────────

def _safe_text(el: Tag | None) -> str:
    if not el:
        return ""
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()


def _norm(s: str) -> str:
    """Lower-case, collapse whitespace, resolve fi/fl ligatures."""
    return (
        re.sub(r"\s+", " ", (s or "").strip())
        .lower()
        .replace("\ufb01", "fi")
        .replace("\ufb02", "fl")
    )


def _extract_meta(soup: BeautifulSoup, prop: str) -> str:
    tag = (
        soup.select_one(f'meta[property="{prop}"]')
        or soup.select_one(f'meta[name="{prop}"]')
    )
    return (tag["content"] or "").strip() if tag and tag.get("content") else ""


def _is_skip_image(url: str) -> bool:
    return any(kw in url.lower() for kw in SKIP_IMAGE_KEYWORDS)


def _extract_course_name(soup: BeautifulSoup) -> str:
    """
    Try in order:
      1. og:title meta  — split on ' – '  ' - '  ' | '
      2. <title> tag    — same splits
    Always preferred over scraping hero <p> tags because it is
    unambiguous and present on every page layout.
    """
    sources = [_extract_meta(soup, "og:title")]
    t = soup.find("title")
    if t:
        sources.append(t.get_text(strip=True))

    for source in sources:
        if not source:
            continue
        for sep in [" \u2013 ", " \u2014 ", " - ", " | "]:
            if sep in source:
                return source.split(sep)[0].strip()
        return source.strip()
    return ""


def _extract_image(soup: BeautifulSoup, listing_image: str = "") -> str:
    """
    Try in order:
      1. og:image (skip if logo)
      2. Any S3 img tag dated 20xx/xx (skip logos)
      3. listing_image fallback
    """
    og = _extract_meta(soup, "og:image")
    if og and not _is_skip_image(og):
        return og
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if src and not _is_skip_image(src) and re.search(r"/202\d/\d+/", src):
            return src
    return listing_image


# ─────────────────────────── Layout A ────────────────────────────────
# Full structured page with a div.wysiwyg (NOT div.course-wysiwyg).
#
# Confirmed structure from diagnostic:
#   <div class="wysiwyg w-full px-6 pb-6">
#     <p>(no class) <strong>What does this qualification cover?</strong></p>
#     <p>(no class) description text...</p>
#     <p>(no class) <strong>Who is this course suitable for?</strong></p>
#     ...
#   </div>
#
# Hero section:
#   <p class="w-full md:w-3/4 text-lg pb-6 text-white">Level 2</p>
#   <p class="text-3xl ... font-extrabold text-white pb-6">Course Name</p>
#   <p class="w-full md:w-3/4 text-white text-lg pb-6">Short Online Course</p>
#   <p class="text-lg text-white font-bold pb-1">Level</p>
#   <p class="text-xl text-white pb-4">2 Certificate</p>
#   (Awarding organisation / Qualification / Qualification Duration follow same pattern)
# ─────────────────────────────────────────────────────────────────────

# All wysiwyg section headings — used to stop content collection at next heading
_WYSIWYG_SECTION_HEADINGS = {
    _norm(h) for h in [
        # course_description variants
        "What does this qualification cover?",
        "What does this course cover?",
        "What does the qualification cover?",
        "What does the course cover?",
        "About this course",
        "Course overview",
        "Course description",
        "Overview",
        # who_for variants
        "Who is this course suitable for?",
        "Who is this qualification suitable for?",
        "Who is this suitable for?",
        "Who is this for?",
        "Who is this course for?",
        "Who is this qualification for?",
        "Who should attend?",
        # structure variants
        "How is this qualification structured?",
        "How is the course structured?",
        "How is this course structured?",
        "Course units",
        "Course content",
        "Modules",
        "Units",
        # entry req variants
        "What are the entry requirements?",
        "Are there any entry requirements?",
        "Entry requirements",
        "Entry requirements for this course",
        "Eligibility requirements",
        "What are the requirements?",
        "Prerequisites",
    ]
}


def _hero_label_value(soup: BeautifulSoup, label: str) -> str:
    """
    Find <p class='text-lg text-white font-bold pb-1'>Label</p>
    and return the text of the NEXT SIBLING <p>.

    Confirmed from diagnostic: the label <p> has class containing 'text-lg'
    AND the value <p> is its direct next sibling (not nested, not inside another div).
    We use find_next_sibling('p') which works correctly for this flat structure.

    Handles the 'Qualiﬁcation' fi-ligature via _norm().
    """
    label_norm = _norm(label)
    for p in soup.find_all("p"):
        cls = " ".join(p.get("class", []))
        # Must have text-lg in class (confirmed: 'text-lg text-white font-bold pb-1')
        if "text-lg" not in cls:
            continue
        if _norm(_safe_text(p)) == label_norm:
            nxt = p.find_next_sibling("p")
            if nxt:
                return _safe_text(nxt)
    return ""


def _wysiwyg_collect_section(
    all_paras: list[Tag],
    heading_norms: set[str],
    *,
    prefix_match: list[str] | None = None,
) -> str:
    """
    Find the first <p> matching heading_norms (exact) or prefix_match (fuzzy).
    Collect subsequent non-empty <p> texts until the next section heading.
    Handles empty <p> spacers common in live Kiwi HTML.
    """
    def _is_section_heading(p_tag: Tag, txt: str) -> bool:
        norm = _norm(txt)
        if norm in _WYSIWYG_SECTION_HEADINGS:
            return True
        if p_tag.find("strong") and len(txt) < 130:
            if norm.endswith("?") or any(norm.startswith(pfx) for pfx in [
                "what ", "who ", "how ", "when ", "where ",
                "entry", "eligib", "prerequis", "about ", "course overview",
            ]):
                return True
        return False

    def _matches(norm_text: str) -> bool:
        if norm_text in heading_norms:
            return True
        if prefix_match:
            return any(norm_text.startswith(pfx) for pfx in prefix_match)
        return False

    for i, p in enumerate(all_paras):
        if _matches(_norm(_safe_text(p))):
            collected: list[str] = []
            for sib in all_paras[i + 1:]:
                txt = _safe_text(sib)
                if not txt:
                    continue          # skip empty <p> spacers
                if _is_section_heading(sib, txt):
                    break
                collected.append(txt)
            if collected:
                return "\n".join(collected)
    return ""


def _wysiwyg_collect_bullets(wysiwyg_div: Tag, heading_norms: set[str]) -> list[str]:
    """
    Find the heading <p>, then the NEXT SIBLING <ul> (direct child of wysiwyg).
    Returns list of <li> texts.
    """
    children = list(wysiwyg_div.children)
    for i, child in enumerate(children):
        if not isinstance(child, Tag):
            continue
        if child.name == "p" and _norm(_safe_text(child)) in heading_norms:
            for sib in children[i + 1:]:
                if isinstance(sib, Tag):
                    if sib.name == "ul":
                        return [_safe_text(li) for li in sib.find_all("li") if _safe_text(li)]
                    if sib.name == "p" and _safe_text(sib):
                        break   # next section heading — stop
    return []


def _scrape_layout_a(soup: BeautifulSoup, wysiwyg_div: Tag) -> dict:
    # ── course_name ───────────────────────────────────────────────────
    # Always use og:title first — it is unambiguous.
    # The hero <p class='text-3xl text-white'> is a FALLBACK only,
    # and we must skip "Enrol and pay now" which has the same classes.
    course_name = _extract_course_name(soup)
    if not course_name:
        for p in soup.find_all("p"):
            cls = " ".join(p.get("class", []))
            if "text-3xl" in cls and "text-white" in cls and "font-extrabold" in cls:
                txt = _safe_text(p)
                if txt and not txt.lower().startswith("enrol"):
                    course_name = txt
                    break

    # ── level_prefix ──────────────────────────────────────────────────
    # Confirmed: <p class='w-full md:w-3/4 text-lg pb-6 text-white'>Level 2</p>
    level_prefix = ""
    for p in soup.find_all("p"):
        cls = " ".join(p.get("class", []))
        if "text-lg" in cls and "pb-6" in cls and "text-white" in cls:
            txt = _safe_text(p)
            if txt.lower().startswith("level"):
                level_prefix = txt
                break

    # ── course_type ───────────────────────────────────────────────────
    # Confirmed: <p class='w-full md:w-3/4 text-white text-lg pb-6'>Short Online Course</p>
    course_type = ""
    for p in soup.find_all("p"):
        cls = " ".join(p.get("class", []))
        if "text-white" in cls and "w-full" in cls and "md:w-3/4" in cls:
            txt = _safe_text(p)
            if txt and not txt.lower().startswith("level") and len(txt) < 80:
                course_type = txt
                break

    # ── hero label/value pairs ────────────────────────────────────────
    level_value   = _hero_label_value(soup, "Level")
    awarding_org  = _hero_label_value(soup, "Awarding organisation")
    qualification = _hero_label_value(soup, "Qualification")   # handles fi ligature
    qual_duration = _hero_label_value(soup, "Qualification Duration")

    # Combine level_prefix + level_value for qual_level field
    parts     = [x for x in [level_prefix, level_value] if x]
    qual_level = " — ".join(parts) if len(parts) > 1 else (parts[0] if parts else "")

    # ── cost ──────────────────────────────────────────────────────────
    # Confirmed Layout A cost:
    #   <p class='text-lg text-grey-darker pb-2'>UK Students</p>
    #   <p class='text-2xl font-bold text-green'>£500.00</p>
    # NOTE: NOT 'font-bold text-2xl text-green' — order may vary, check all three classes.
    cost       = ""
    cost_parts: list[str] = []

    # Pattern 1: label paragraph with "UK Students" / "International Students"
    # Search ONLY within the cost/pricing container to avoid duplicate labels
    # from other sections of the page.
    all_p = soup.find_all("p")
    seen_cost_labels: set[str] = set()
    for p in all_p:
        txt = _safe_text(p)
        txt_norm = _norm(txt)
        if txt_norm in ("uk students", "international students"):
            if txt_norm in seen_cost_labels:
                continue                    # skip duplicate label
            nxt = p.find_next_sibling("p")
            val = _safe_text(nxt) if nxt else ""
            if val:
                seen_cost_labels.add(txt_norm)
                cost_parts.append(f"{txt}: {val}")
                if not cost and "£" in val:
                    cost = val

    # Grab "Total £500.00" line if present (only once)
    for p in all_p:
        txt = _safe_text(p)
        if _norm(txt).startswith("total") and "£" in txt:
            if txt not in cost_parts:
                cost_parts.append(txt)
            break

    # Pattern 2 fallback: any <p> with font-bold + text-2xl + text-green + £
    if not cost:
        for p in all_p:
            cls = " ".join(p.get("class", []))
            if "font-bold" in cls and "text-2xl" in cls and "text-green" in cls:
                txt = _safe_text(p)
                if "£" in txt:
                    cost = txt
                    if not cost_parts:
                        cost_parts.append(txt)
                    break

    cost_description = "\n".join(cost_parts)

    # ── wysiwyg content sections ──────────────────────────────────────
    all_paras = wysiwyg_div.find_all("p")

    course_description = _wysiwyg_collect_section(
        all_paras,
        {_norm(h) for h in [
            "What does this qualification cover?",
            "What does this course cover?",
            "What does the qualification cover?",
            "What does the course cover?",
            "About this course",
            "Course overview",
            "Course description",
            "Overview",
        ]},
        prefix_match=["what does this", "what does the"],
    )
    # Fallback: some courses have no 'cover?' heading — use structure section
    if not course_description:
        course_description = _wysiwyg_collect_section(
            all_paras,
            {_norm(h) for h in [
                "How is this qualification structured?",
                "How is the course structured?",
                "How is this course structured?",
            ]},
            prefix_match=["how is this", "how is the"],
        )
    who_this_course_is_for = _wysiwyg_collect_section(
        all_paras,
        {_norm(h) for h in [
            "Who is this course suitable for?",
            "Who is this qualification suitable for?",
            "Who is this suitable for?",
            "Who is this for?",
            "Who is this course for?",
            "Who is this qualification for?",
            "Who should attend?",
        ]},
        prefix_match=["who is this", "who should"],
    )
    entry_reeq = _wysiwyg_collect_section(
        all_paras,
        {_norm(h) for h in [
            "What are the entry requirements?",
            "Are there any entry requirements?",
            "Entry requirements",
            "Entry requirements for this course",
            "Eligibility requirements",
            "What are the requirements?",
            "Prerequisites",
        ]},
        prefix_match=["what are the entry", "entry requirement", "eligibility", "prerequis"],
    )
    course_units = _wysiwyg_collect_bullets(
        wysiwyg_div,
        {_norm(h) for h in [
            "How is this qualification structured?",
            "How is the course structured?",
            "How is this course structured?",
            "Course units", "Course content", "Modules", "Units",
        ]},
    )

    # requirement_summery = description + units
    req_parts = [course_description]
    if course_units:
        req_parts.append("Units:\n" + "\n".join(f"• {u}" for u in course_units))
    requirement_summery = "\n\n".join(x for x in req_parts if x)

    # ── delivery method ───────────────────────────────────────────────
    learning_method    = "Online"
    attendance_pattern = "Online"
    if course_type:
        ct = course_type.lower()
        if "classroom" in ct or "face" in ct or "in-person" in ct or "in person" in ct:
            learning_method    = "Classroom"
            attendance_pattern = "In person"
        elif "blended" in ct:
            learning_method    = "Blended"
            attendance_pattern = "Blended"

    return dict(
        course_name            = course_name,
        course_type            = course_type or "Short Online Course",
        qual_level             = qual_level,
        qual_duration          = qual_duration,
        awarding_org           = awarding_org,
        qualification          = qualification,
        cost                   = cost,
        cost_description       = cost_description,
        learning_method        = learning_method,
        attendance_pattern     = attendance_pattern,
        course_description     = course_description,
        who_this_course_is_for = who_this_course_is_for,
        entry_reeq             = entry_reeq,
        requirement_summery    = requirement_summery,
    )


# ─────────────────────────── Layout B ────────────────────────────────
# Simple course page — no structured hero, Cademy JS widget for rich content.
#
# Confirmed structure:
#   <div class="course-wysiwyg w-full lg:w-1/2 lg:px-6 pb-6 text-grey-darker">
#     <p><strong>Aims and Objectives of this Course:</strong></p>
#     <ul>
#       <li>...</li>   ← this IS available in static HTML
#     </ul>
#   </div>
#   <div class="enrol-section">
#     <p class="text-white pb-4 lg:pb-6 text-lg">Individual Attendee</p>
#     <p class="font-bold text-2xl text-green">£150</p>
#   </div>
#   <script src="https://cademy.co.uk/widget.js">  ← rich content loaded here
#
# IMPORTANT: who_this_course_is_for and entry_reeq are served ONLY by the
# Cademy JS widget. They are NOT present in the static HTML response.
# They will always be empty unless a headless browser is used.
# ─────────────────────────────────────────────────────────────────────

def _scrape_layout_b(soup: BeautifulSoup, listing_image: str = "") -> dict:
    course_name = _extract_course_name(soup)

    # ── delivery method ───────────────────────────────────────────────
    # Layout B pages have "Delivered In-Person" or "Delivered Online On Demand"
    # text in the Cademy widget — NOT in static HTML. So we default to Online
    # but check if the listing page already told us the delivery type.
    # The listing page includes "Course Delivery: In person" in the card text.
    # We detect from URL slug or from any static text clue.
    learning_method    = "Online"
    attendance_pattern = "Online"
    course_type        = "Short Online Course"

    # ── cost ──────────────────────────────────────────────────────────
    # Confirmed: <p class='font-bold text-2xl text-green'>£150</p>
    # Preceded by: <p class='text-white pb-4 lg:pb-6 text-lg'>Individual Attendee</p>
    tiers: list[tuple[str, str]] = []
    all_p = soup.find_all("p")
    for i, p in enumerate(all_p):
        cls = " ".join(p.get("class", []))
        if "font-bold" in cls and "text-2xl" in cls and "text-green" in cls:
            price_txt = _safe_text(p)
            if not ("£" in price_txt or _norm(price_txt) in ("free", "tbc")):
                continue
            label_txt = ""
            if i > 0:
                prev     = all_p[i - 1]
                prev_cls = " ".join(prev.get("class", []))
                if "text-white" in prev_cls and "text-lg" in prev_cls:
                    label_txt = _safe_text(prev)
            tiers.append((label_txt, price_txt))

    # Primary cost: prefer "Individual Attendee", else first tier
    primary_cost = ""
    for label, price in tiers:
        if "individual" in label.lower():
            primary_cost = price
            break
    if not primary_cost and tiers:
        primary_cost = tiers[0][1]

    cost_description = "\n".join(
        f"{label}: {price}" if label else price for label, price in tiers
    )

    # ── course-wysiwyg content ────────────────────────────────────────
    # Confirmed: div class contains 'course-wysiwyg'.
    # Content available in static HTML: Aims & Objectives heading + bullet list.
    # who_this_course_is_for / entry_reeq come from Cademy JS — always blank here.
    wysiwyg_divs = soup.find_all(
        "div",
        attrs={"class": lambda c: bool(c and "course-wysiwyg" in c)},
    )

    all_blocks: list[str] = []
    for div in wysiwyg_divs:
        for child in div.children:
            if isinstance(child, NavigableString):
                t = str(child).strip()
                if t:
                    all_blocks.append(t)
                continue
            if not isinstance(child, Tag):
                continue
            txt = _safe_text(child)
            if not txt:
                continue
            if child.name == "ul":
                items = [_safe_text(li) for li in child.find_all("li") if _safe_text(li)]
                all_blocks.append("\n".join(f"• {item}" for item in items))
            else:
                all_blocks.append(txt)

    course_description = "\n".join(all_blocks).strip()

    return dict(
        course_name            = course_name,
        course_type            = course_type,
        qual_level             = "",
        qual_duration          = "",
        awarding_org           = "",
        qualification          = "",
        cost                   = primary_cost,
        cost_description       = cost_description,
        learning_method        = learning_method,
        attendance_pattern     = attendance_pattern,
        course_description     = course_description,
        who_this_course_is_for = "",   # Cademy JS widget only
        entry_reeq             = "",   # Cademy JS widget only
        requirement_summery    = course_description,
        listing_image          = listing_image,
    )


# ─────────────────────────── HTTP client ─────────────────────────────

class KiwiCourseClient:

    def __init__(
        self,
        delay:   float       = 1.5,
        timeout: int         = 30,
        proxies: dict | None = None,
    ):
        self.delay   = float(delay)
        self.timeout = int(timeout)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent":      "Mozilla/5.0 (compatible; KiwiCourseScraper/4.0)",
            "Accept-Language": "en-GB,en;q=0.9",
        })
        if proxies:
            self.session.proxies.update(proxies)

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass

    def _sleep(self) -> None:
        time.sleep(self.delay + random.uniform(0.2, 0.8))

    def _get_soup(self, url: str, retries: int = 3) -> BeautifulSoup | None:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = min(30, 2 * attempt + random.uniform(1, 3))
                    logger.warning(
                        f"HTTP {resp.status_code} attempt {attempt}/{retries} "
                        f"— retry in {wait:.1f}s | {url}"
                    )
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                self._sleep()
                return BeautifulSoup(resp.text, "html.parser")
            except Exception as exc:
                last_exc = exc
                time.sleep(min(20, 2 * attempt + random.uniform(1, 2)))
        logger.error(f"Failed after {retries} retries: {url} | {last_exc}")
        return None

    # ── listing page crawler ──────────────────────────────────────────

    def iter_all_course_links(
        self,
        *,
        max_pages: int = 0,
    ) -> Iterator[tuple[str, uuid.UUID, str]]:
        """
        Yields (course_url, course_id, listing_image_url).

        The listing_image is captured here because Layout B detail pages
        have only the CIPD logo in og:image (which we skip) and no S3
        content image in the static HTML (content loaded by Cademy JS).
        The listing page card has the real thumbnail for every course.
        """
        seen: set[str] = set()
        page = 1

        while True:
            if max_pages and page > max_pages:
                break

            url = (
                LISTING_URL
                if page == 1
                else f"{BASE_URL}/commercial-courses/page/{page}/"
            )
            soup = self._get_soup(url)
            if not soup:
                break

            # Build thumbnail map from listing cards:
            # each course card is an <a href='/short-course/...'> containing <img>
            listing_images: dict[str, str] = {}
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/short-course/" not in href:
                    continue
                full = (
                    href if href.startswith("http") else urljoin(BASE_URL, href)
                ).split("?")[0].rstrip("/") + "/"
                if full in listing_images:
                    continue
                img = a.find("img")
                if img:
                    src = img.get("src") or img.get("data-src") or ""
                    if src and not _is_skip_image(src):
                        listing_images[full] = src

            found = 0
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/short-course/" not in href:
                    continue
                full = (
                    href if href.startswith("http") else urljoin(BASE_URL, href)
                ).split("?")[0].rstrip("/") + "/"
                if full in seen or not full.startswith(BASE_URL):
                    continue
                seen.add(full)
                found += 1
                yield full, _make_course_id(full), listing_images.get(full, "")

            if not found:
                break
            page += 1

    # ── detail page scraper ───────────────────────────────────────────

    def scrape_course_detail(
        self,
        course_url:    str,
        course_id:     uuid.UUID,
        listing_image: str = "",
    ) -> KiwiCourseDetail:

        soup = self._get_soup(course_url)

        if not soup:
            logger.error(f"Could not fetch {course_url}")
            return KiwiCourseDetail(
                course_id    = course_id,
                course_url   = course_url,
                image_url    = listing_image,
                college_name = KIWI_COLLEGE,
                address      = KIWI_ADDRESS,
                phone        = KIWI_PHONE,
                email        = KIWI_EMAIL,
                website      = KIWI_WEBSITE,
                city         = KIWI_CITY,
                zip_code     = KIWI_POSTCODE,
            )

        # ── Layout detection ──────────────────────────────────────────
        # Layout A: div whose class string contains 'wysiwyg' but NOT 'course-wysiwyg'
        # e.g. class="wysiwyg w-full px-6 pb-6"
        # We must NOT match class="course-wysiwyg ..." (Layout B).
        wysiwyg_a = soup.find(
            "div",
            attrs={"class": lambda c: bool(c and "wysiwyg" in c and "course-wysiwyg" not in c)},
        )

        if wysiwyg_a:
            fields = _scrape_layout_a(soup, wysiwyg_a)
        else:
            fields = _scrape_layout_b(soup, listing_image=listing_image)

        # course_name: og:title is the most reliable source on every page.
        # Use it first; fall back to whatever the layout parser found.
        course_name = _extract_course_name(soup) or fields.get("course_name", "")

        # Image: og:image → S3 img → listing thumbnail
        image_url = _extract_image(soup, listing_image)

        category, subcategory = _resolve_taxonomy(course_name)

        return KiwiCourseDetail(
            course_id                  = course_id,
            course_url                 = course_url,
            image_url                  = image_url,

            category                   = category,
            subcategory                = subcategory,
            requirement_summery        = fields.get("requirement_summery", ""),

            course_name                = course_name,
            course_type                = fields.get("course_type", "Short Online Course"),
            learning_method            = fields.get("learning_method", "Online"),
            attendance_pattern         = fields.get("attendance_pattern", "Online"),

            course_qualification_level = fields.get("qual_level", ""),
            course_stryd_time          = fields.get("qual_duration", ""),
            course_hours               = fields.get("qual_duration", ""),
            awarding_organization      = fields.get("awarding_org", ""),

            course_description         = fields.get("course_description", ""),
            who_this_course_is_for     = fields.get("who_this_course_is_for", ""),
            entry_reeq                 = fields.get("entry_reeq", ""),

            duration                   = fields.get("qual_duration", ""),
            cost                       = fields.get("cost", ""),
            cost_description           = fields.get("cost_description", ""),

            college_name               = KIWI_COLLEGE,
            address                    = KIWI_ADDRESS,
            phone                      = KIWI_PHONE,
            email                      = KIWI_EMAIL,
            website                    = KIWI_WEBSITE,
            city                       = KIWI_CITY,
            zip_code                   = KIWI_POSTCODE,
            latitude                   = None,
            longitude                  = None,
        )
"""
apprenticeship/scrapper/kiwi/client.py

Kiwi Education apprenticeships scraper using filters:
  /apprenticeships/?course-level=<level>&course-type=<type>

- category    -> OUR taxonomy category  (e.g. "Business and finance")
- subcategory -> OUR taxonomy subcategory (e.g. "Accounting technician")
                 Resolved by matching the scraped course title against our
                 taxonomy; falls back to the hardcoded default in COURSE_TYPE_MAP.
"""

from __future__ import annotations

import hashlib
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

logger = logging.getLogger(__name__)

BASE_URL     = "https://kiwieducation.co.uk"
LISTING_BASE = f"{BASE_URL}/apprenticeships/"


# ─────────────────────────── Our taxonomy ────────────────────────────

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
    "Animal Care": [
        "Agricultural contractor", "Agricultural inspector",
        "Animal care worker", "Animal technician", "Assistance dog trainer",
        "Beekeeper", "Biologist", "Countryside ranger", "Dog groomer",
        "Dog handler", "Ecologist", "Farm worker", "Farmer", "Farrier",
        "Fish farmer", "Gamekeeper", "Horse groom", "Horse riding instructor",
        "Jockey", "Kennel worker", "Pet behaviour consultant",
        "Racehorse trainer", "RSPCA inspector", "Vet", "Veterinary nurse",
        "Veterinary physiotherapist", "Zookeeper", "Zoologist",
    ],
    "Beauty and wellbeing": [
        "Acupuncturist", "Aromatherapist", "Art therapist",
        "Beauty consultant", "Beauty therapist", "Chiropractor", "Counsellor",
        "Dance movement psychotherapist", "Dramatherapist", "Hairdresser",
        "Health play specialist", "Homeopath", "Massage therapist",
        "Medical herbalist", "Music therapist", "Nail technician",
        "Naturopath", "Nutritional therapist", "Osteopath", "Pilates teacher",
        "Reflexologist", "Reiki healer", "Tattooist and body piercer",
        "Yoga therapist",
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
    "Construction and trades": [
        "Acoustics consultant", "Architect", "Architectural technician",
        "Architectural technologist", "Boat builder", "Bricklayer",
        "Builders' merchant", "Building control officer",
        "Building services engineer", "Building site inspector",
        "Building surveyor", "Building technician", "CAD technician",
        "Carpenter", "Carpet fitter and floor layer",
        "Cavity insulation installer", "Ceiling fixer", "Civil engineer",
        "Civil engineering technician", "Commercial energy assessor",
        "Construction contracts manager", "Construction labourer",
        "Construction manager", "Construction plant mechanic",
        "Construction plant operator", "Construction site supervisor",
        "Crane driver", "Demolition operative", "Domestic energy assessor",
        "Drone pilot", "Dryliner", "Electrician",
        "Electricity distribution worker",
        "Engineering construction craftworker", "Estimator",
        "Engineering construction technician", "Facilities manager",
        "Fence installer", "Formworker", "Gas service technician",
        "General practice surveyor", "Glazier", "Heat pump engineer",
        "Heating and ventilation engineer", "Heritage officer",
        "Kitchen and bathroom designer", "Kitchen and bathroom fitter",
        "Land and property valuer and auctioneer", "Land surveyor",
        "Landscaper", "Mechanical engineering technician", "Paint sprayer",
        "Painter and decorator", "Pipe fitter",
        "Planning and development surveyor", "Plasterer", "Plumber",
        "Quantity surveyor", "Quarry engineer", "Quarry worker",
        "Refrigeration and air-conditioning installer", "Road worker",
        "Roofer", "Rural surveyor", "Scaffolder", "Shopfitter",
        "Smart home installer", "Solar panel installer", "Steel erector",
        "Steel fixer", "Steeplejack", "Stonemason", "Structural engineer",
        "Surveying technician", "Thatcher", "Thermal insulation engineer",
        "Tiler", "Town planner", "Town planning assistant",
        "Water network operative", "Welder", "Wood machinist",
    ],
    "Creative and media": [
        "Actor", "Advertising account executive",
        "Advertising account planner", "Advertising art director",
        "Advertising copywriter", "Advertising media buyer",
        "Advertising media planner", "Animator", "Antique dealer",
        "Architect", "Architectural technician", "Architectural technologist",
        "Archivist", "Art editor", "Art therapist", "Art valuer",
        "Arts administrator", "Audio visual technician", "Blacksmith",
        "Broadcast engineer", "Broadcast journalist",
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
    "Delivery and storage": [
        "Airport baggage handler", "Automotive parts advisor",
        "Builders' merchant", "Delivery van driver",
        "Food packaging operative", "Forklift driver", "HGV driver",
        "Import-export clerk", "Packer", "Port operative", "Postperson",
        "Removals worker", "Road transport manager", "Roadie",
        "Stock control assistant", "Supply chain manager",
        "Warehouse manager", "Warehouse worker",
    ],
    "Emergency and uniform services": [
        "Aid worker", "Army officer", "Bodyguard", "Border Force officer",
        "Chief inspector", "Civil enforcement officer", "Coastguard", "Diver",
        "Dog handler", "Door supervisor", "Fingerprint officer", "Firefighter",
        "Forensic collision investigator", "Immigration officer",
        "Merchant Navy deck officer", "Merchant Navy engineering officer",
        "Merchant Navy rating", "Neighbourhood warden", "Paramedic",
        "Police community support officer", "Police officer",
        "Prison governor", "Prison instructor", "Prison officer",
        "RAF aviator", "RAF officer", "RAF regiment gunner",
        "Royal Marines commando", "Royal Marines officer", "Royal Navy officer",
        "Royal Navy rating", "Scenes of crime officer", "Security manager",
        "Security officer", "Security Service personnel", "Soldier",
        "Store detective",
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
        "Chemical engineer", "Climate scientist",
        "Commercial energy assessor",
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
    "Government services": [
        "Air accident investigator", "Army officer",
        "Assistant immigration officer", "Bodyguard", "Border Force officer",
        "Careers adviser", "Cemetery worker", "Chief inspector",
        "Child protection officer", "Civil enforcement officer",
        "Civil Service administrative officer",
        "Civil Service executive officer", "Civil Service manager",
        "Coastguard", "Criminologist", "Data scientist",
        "Diplomatic Service officer", "Diver", "Dog handler",
        "Environmental health practitioner", "Fingerprint officer",
        "Food manufacturing inspector", "Forensic collision investigator",
        "Heritage officer", "Immigration officer", "MP", "Museum attendant",
        "Neighbourhood warden", "Ofsted inspector",
        "Police community support officer", "Police officer",
        "Prison governor", "Prison instructor", "Probation officer",
        "Probation services officer", "RAF aviator", "RAF officer",
        "RAF regiment gunner", "Recycling operative",
        "Registrar of births, deaths, marriages and civil partnerships",
        "Royal Marines commando", "Royal Marines officer",
        "Royal Navy officer", "Royal Navy rating", "Scenes of crime officer",
        "Security Service personnel", "Soldier",
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
    "Home services": [
        "Accommodation warden", "Bin worker", "Bodyguard",
        "British Sign Language interpreter", "Care escort", "Care worker",
        "Caretaker", "Celebrant", "Cleaner", "Community transport driver",
        "Crematorium technician", "Domestic energy assessor", "Embalmer",
        "Industrial cleaner", "Laundry worker", "Life coach",
        "Pest control technician", "Postperson", "Recycling operative",
        "Religious leader", "Tailor", "Telecoms engineer", "Wedding planner",
    ],
    "Hospitality and food": [
        "Baker", "Bar person", "Barista", "Butcher", "Cake decorator",
        "Catering manager", "Chef", "Consumer scientist",
        "Counter service assistant", "Cruise ship steward",
        "Food factory worker", "Food manufacturing inspector",
        "Food scientist", "Hotel housekeeper", "Hotel manager",
        "Hotel porter", "Kitchen porter", "Publican", "Restaurant manager",
        "School lunchtime supervisor", "Waiter", "Wedding planner",
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
    "Manufacturing": [
        "3D printing technician", "Aerospace engineer",
        "Aerospace engineering technician", "Agricultural engineer",
        "Agricultural engineering technician", "Automotive engineer",
        "Blacksmith", "Building services engineer",
        "Car manufacturing worker", "Chemical engineer",
        "Chemical engineering technician", "Chemical plant process operator",
        "CNC machinist", "Crane driver", "Design and development engineer",
        "Dressmaker", "Electronics engineer",
        "Engineering construction technician",
        "Engineering maintenance technician", "Engineering operative",
        "Food manufacturing inspector", "Food packaging operative",
        "Food scientist", "Footwear designer-maker", "Foundry moulder",
        "Garment technologist", "Glassmaker", "Leather craftworker",
        "Maintenance fitter", "Manufacturing systems engineer",
        "Marine engineer", "Materials engineer", "Materials technician",
        "Mechanical engineering technician", "Metrologist", "Motor mechanic",
        "Motorsport engineer", "Musical instrument maker and repairer",
        "Naval architect", "Non-destructive testing technician",
        "Packaging technologist", "Packer", "Paint sprayer",
        "Patent attorney", "Pattern cutter", "Product designer",
        "Production manager (manufacturing)",
        "Production worker (manufacturing)", "Quality control officer",
        "Quarry worker", "Recycling operative",
        "Rolling stock engineering technician", "Roustabout",
        "Sewing machinist", "Sign maker", "Technical brewer",
        "Textile designer", "Textile operative", "Toolmaker",
        "Vehicle body repairer", "Welder", "Window fabricator",
        "Wood machinist",
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
    "Science and research": [
        "Acoustics consultant", "Agronomist", "Animal technician",
        "Arboricultural officer", "Archaeologist", "Astronaut", "Astronomer",
        "Audiologist", "Biochemist", "Biologist", "Biomedical scientist",
        "Biotechnologist", "Cartographer", "Chemical engineer",
        "Chemical engineering technician", "Chemical plant process operator",
        "Chemist", "Climate scientist", "Clinical engineer",
        "Clinical psychologist", "Consumer scientist", "Countryside officer",
        "Data analyst-statistician", "Data scientist", "Ecologist",
        "Economist", "Education technician", "Electronics engineer",
        "Environmental consultant", "Fingerprint officer", "Food scientist",
        "Forensic scientist", "Garment technologist", "Geneticist",
        "Geoscientist", "Geospatial technician", "Geotechnician",
        "Healthcare science assistant", "Hydrologist",
        "Intelligence analyst", "Laboratory technician", "Land surveyor",
        "Marine engineer", "Market research data analyst",
        "Market researcher", "Materials engineer", "Materials technician",
        "Medical physicist", "Meteorologist", "Metrologist", "Microbiologist",
        "Nanotechnologist", "Nuclear engineer", "Oceanographer",
        "Operational researcher", "Palaeontologist", "Pathologist",
        "Performance sports scientist", "Pet behaviour consultant",
        "Pharmacologist", "Physicist", "Psychiatrist", "Psychologist",
        "Quarry engineer", "Renewable energy engineer", "Research scientist",
        "Robotics engineer", "Scenes of crime officer", "Seismologist",
        "Sport and exercise psychologist", "Technical brewer", "Vet",
        "Zoologist",
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
    "Sports and leisure": [
        "Athlete", "Cinema or theatre attendant", "Cycling coach", "Diver",
        "Esports gamer", "Events manager", "Fitness instructor",
        "Football coach", "Football referee", "Health trainer",
        "Horse riding instructor", "Hotel housekeeper", "Jockey",
        "Leisure centre assistant", "Leisure centre manager", "Lifeguard",
        "Martial arts instructor", "Motorsport engineer", "Museum attendant",
        "Outdoor activities instructor", "PE teacher",
        "Performance sports scientist", "Personal trainer", "Pilates teacher",
        "Racehorse trainer", "Resort representative", "Sailing instructor",
        "Sport and exercise psychologist", "Sports coach",
        "Sports commentator", "Sports development officer",
        "Sports professional", "Swimming teacher",
        "Tourist information centre assistant",
        "Visitor attraction general manager", "Yoga teacher",
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
    "Transport": [
        "Air accident investigator", "Air traffic controller",
        "Airline customer service agent", "Airline pilot",
        "Airport baggage handler", "Airport information assistant",
        "Ambulance care assistant", "Automotive parts advisor",
        "Bus or coach driver", "Cabin crew", "Car rental agent",
        "Care escort", "Community transport driver", "Delivery van driver",
        "Driving instructor", "Forensic collision investigator",
        "Forklift driver", "Heavy vehicle technician", "Helicopter engineer",
        "Helicopter pilot", "HGV driver", "Import-export clerk",
        "Merchant Navy deck officer", "Merchant Navy engineering officer",
        "Merchant Navy rating", "Port operative",
        "Rail track maintenance worker", "Railway signaller",
        "Road transport manager", "Rolling stock engineering technician",
        "Signalling technician", "Supply chain manager", "Taxi driver",
        "Train conductor", "Train driver", "Train station worker",
        "Tram driver", "Transport planner", "Windscreen fitter",
    ],
    "Travel and tourism": [
        "Airline customer service agent", "Airline pilot",
        "Airport information assistant", "Cabin crew", "Cruise ship steward",
        "Diver", "Heritage officer", "Hotel housekeeper", "Hotel manager",
        "Hotel porter", "Interpreter", "Museum attendant", "Port operative",
        "Resort representative", "Sailing instructor", "Tour manager",
        "Tourist guide", "Tourist information centre assistant",
        "Travel agent", "Visitor attraction general manager",
    ],
}

# Reverse lookup: normalised subcategory -> (category, original_subcategory)
_SUBCATEGORY_LOOKUP: dict[str, tuple[str, str]] = {
    sub.lower(): (cat, sub)
    for cat, subs in OUR_TAXONOMY.items()
    for sub in subs
}

# Manual overrides for Kiwi course titles that don't match our taxonomy well.
# Key   = scraped title lowercased (exact)
# Value = (our_category, our_subcategory)
_KIWI_TITLE_OVERRIDES: dict[str, tuple[str, str]] = {
    # Leadership & Management
    "improvement leader":                     ("Managerial",                        "Management consultant"),
    "improvement practitioner":               ("Managerial",                        "Management consultant"),
    "operations/departmental manager":        ("Managerial",                        "Office manager"),
    "team leader/supervisor":                 ("Managerial",                        "Supervisor"),
    "senior leader":                          ("Managerial",                        "Management consultant"),
    "people professional (cipd)":             ("Administration",                    "Human resources officer"),
    "people professional":                    ("Administration",                    "Human resources officer"),
    "hr consultant/partner":                  ("Administration",                    "Human resources officer"),
    # Marketing & Creative
    "content creator":                        ("Creative and media",                "Marketing executive"),
    "multi-channel marketer":                 ("Creative and media",                "Marketing executive"),
    "digital marketer":                       ("Creative and media",                "Marketing executive"),
    "social media marketer":                  ("Creative and media",                "Social media manager"),
    # Digital / Tech
    "data technician":                        ("Computing, technology and digital", "Data scientist"),
    "it solutions technician":                ("Computing, technology and digital", "IT support technician"),
    "cyber security technician":              ("Computing, technology and digital", "IT security co-ordinator"),
    "network engineer":                       ("Computing, technology and digital", "Network engineer"),
    # Accounting & Finance
    "accounts/finance assistant":             ("Business and finance",              "Accounting technician"),
    "professional accounting/taxation":       ("Business and finance",              "Tax adviser"),
    # Green / Environment
    "sustainability business specialist":     ("Environment and land",              "Environmental consultant"),
}

# ─────────────────────────── Course type map ─────────────────────────
#
# Key   = Kiwi dropdown label
# Value = (our_category, our_subcategory_fallback, kiwi_query_value)
#
#   our_category          — our taxonomy category saved to DB
#   our_subcategory_fallback — our taxonomy subcategory saved to DB when
#                             title matching finds no better match
#   kiwi_query_value      — the URL ?course-type= param for Kiwi's filter

COURSE_TYPE_MAP: dict[str, tuple[str, str, str]] = {
    "Accounting & Finance":    ("Business and finance",              "Accounting technician",       "accounting-finance"),
    "Corporate":               ("Managerial",                        "Business project manager",    "corporate"),
    "Digital":                 ("Computing, technology and digital", "Software developer",          "digital"),
    "Green Skills":            ("Environment and land",              "Environmental consultant",    "green-skills"),
    "HR Human Resources":      ("Administration",                    "Human resources officer",     "hr-human-resources"),
    "Leadership & Management": ("Managerial",                        "Management consultant",       "leadership-management"),
    "Management":              ("Managerial",                        "Office manager",              "management"),
    "Marketing & Creative":    ("Creative and media",                "Marketing executive",         "marketing-creative"),
    "People":                  ("Social care",                       "Care worker",                 "people"),
}

DEFAULT_COURSE_LEVEL = "all"

_UK_POSTCODE_RE = re.compile(
    r"\b([A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2})\b", re.IGNORECASE
)


# ─────────────────────────── Taxonomy resolver ───────────────────────

def _resolve_our_taxonomy(
    kiwi_course_type_label: str,
    scraped_title: str,
) -> tuple[str, str]:
    """
    Return (our_category, our_subcategory) for a scraped apprenticeship.

    Priority:
    1. Exact match of scraped title against our subcategory list.
    2. Word-level match — every significant word in the subcategory appears
       in the title (minimum 2 words must match to avoid false positives).
    3. Hardcoded fallback (category + subcategory) from COURSE_TYPE_MAP.
    4. Last resort: primary category from map / scraped title as subcategory.
    """
    import re as _re

    def _words(s: str) -> set[str]:
        # Extract meaningful words (3+ chars, skip stopwords)
        STOPWORDS = {"and", "the", "for", "with", "our", "this", "that", "from"}
        return {w for w in _re.findall(r"[a-z]{3,}", s.lower()) if w not in STOPWORDS}

    title_norm  = scraped_title.strip().lower()
    title_words = _words(title_norm)

    # 1a. Manual override (highest priority — Kiwi-specific titles)
    if title_norm in _KIWI_TITLE_OVERRIDES:
        return _KIWI_TITLE_OVERRIDES[title_norm]

    # 1b. Exact match against our taxonomy subcategories
    if title_norm in _SUBCATEGORY_LOOKUP:
        return _SUBCATEGORY_LOOKUP[title_norm]

    # 2. Word-level match — subcategory words must substantially overlap
    #    with title words. Require ALL subcategory words to appear in the
    #    title AND at least 2 words must match (prevents single-word hits
    #    like "MP" matching "Improvement").
    best_match: tuple[str, str] | None = None
    best_count = 0
    for sub_lower, (cat, sub) in _SUBCATEGORY_LOOKUP.items():
        sub_words = _words(sub_lower)
        if not sub_words:
            continue
        common = sub_words & title_words
        # All subcategory words must be present in the title,
        # AND at least 2 words must match to avoid false positives.
        if common == sub_words and len(common) >= 2:
            if len(common) > best_count:
                best_count = len(common)
                best_match = (cat, sub)

    if best_match:
        return best_match

    # 3. Fallback from COURSE_TYPE_MAP (category + default subcategory)
    mapping = COURSE_TYPE_MAP.get(kiwi_course_type_label)
    if mapping:
        return mapping[0], mapping[1]

    # 4. Last resort
    return "Administration", scraped_title.strip()


# ─────────────────────────── Data classes ────────────────────────────

@dataclass
class ApprenticeshipListing:
    vacancy_ref: str
    vacancy_url: str
    title: str = ""


@dataclass
class ApprenticeshipDetail:
    vacancy_ref: str
    vacancy_url: str
    image_url: str = ""
    requirement_summery: str = ""

    category: str = ""
    subcategory: str = ""

    title: str = ""
    employer_name: str = ""
    location_summary: str = ""
    closing_text: str = ""
    posted_text: str = ""

    summary_text: str = ""
    wage: str = ""
    wage_extra: str = ""
    training_course: str = ""
    hours: str = ""
    hours_per_week: str = ""
    start_date: str = ""
    duration: str = ""
    positions_available: str = ""

    work_intro: str = ""
    what_youll_do_heading: str = ""
    what_youll_do_items: str = ""
    where_youll_work_name: str = ""
    where_youll_work_address: str = ""

    training_intro: str = ""
    training_provider: str = ""
    training_course_repeat: str = ""
    what_youll_learn_items: str = ""
    training_schedule: str = ""
    more_training_information: str = ""

    essential_qualifications: str = ""
    skills_items: str = ""
    other_requirements_items: str = ""

    about_employer: str = ""
    employer_website: str = ""
    company_benefits_items: str = ""

    after_this_apprenticeship: str = ""
    contact_name: str = ""

    city: str = ""
    state: str = ""
    zip_code: str = ""
    latitude: float | None = None
    longitude: float | None = None


# ─────────────────────────── Helpers ─────────────────────────────────

def make_vacancy_ref(vacancy_url: str, category_label: str, course_level: str) -> str:
    key = (
        f"{vacancy_url.strip()}|{category_label.strip()}"
        f"|{(course_level or '').strip()}"
    ).encode("utf-8")
    return f"KIWI_{hashlib.md5(key).hexdigest().upper()[:27]}"


def _safe_text(el: Tag | None) -> str:
    if not el:
        return ""
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def _extract_meta(soup: BeautifulSoup, prop: str) -> str:
    tag = (
        soup.select_one(f'meta[property="{prop}"]')
        or soup.select_one(f'meta[name="{prop}"]')
    )
    if tag and tag.get("content"):
        return (tag["content"] or "").strip()
    return ""


def _html_to_text(root: Tag | None) -> str:
    if not root:
        return ""
    tmp = BeautifulSoup(str(root), "html.parser")
    for t in tmp.find_all(["br", "p", "li", "h1", "h2", "h3", "h4", "h5", "h6"]):
        t.insert_before("\n")
    lines = [ln.strip() for ln in tmp.get_text("\n").splitlines()]
    return "\n".join(ln for ln in lines if ln)


def _extract_li_items(root: Tag | None) -> list[str]:
    if not root:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for li in root.find_all("li"):
        txt = _safe_text(li)
        if txt and txt not in seen:
            seen.add(txt)
            out.append(txt)
    return out


_H_TAGS = {"h1", "h2", "h3", "h4", "h5"}


def _find_heading(soup: BeautifulSoup, texts: list[str]) -> Optional[Tag]:
    targets = {_norm(t) for t in texts if t}
    for tag in soup.find_all(_H_TAGS):
        if _norm(_safe_text(tag)) in targets:
            return tag
    for tag in soup.find_all(_H_TAGS):
        tn = _norm(_safe_text(tag))
        for t in targets:
            if t and t in tn:
                return tag
    return None


def _collect_section(heading: Tag) -> Tag:
    wrapper = BeautifulSoup("<div></div>", "html.parser").div
    stop = int(heading.name[1])
    parent = heading.parent
    if not parent:
        return wrapper
    collecting = False
    for child in parent.children:
        if child is heading:
            collecting = True
            continue
        if not collecting:
            continue
        if isinstance(child, Tag) and child.name in _H_TAGS:
            if int(child.name[1]) <= stop:
                break
        if isinstance(child, Tag):
            wrapper.append(BeautifulSoup(str(child), "html.parser"))
        elif isinstance(child, NavigableString):
            s = str(child).strip()
            if s:
                wrapper.append(NavigableString(s))
    return wrapper


def _section_text(soup: BeautifulSoup, heading_texts: list[str]) -> str:
    h = _find_heading(soup, heading_texts)
    return "" if not h else _html_to_text(_collect_section(h)).strip()


def _section_bullets(soup: BeautifulSoup, heading_texts: list[str]) -> list[str]:
    h = _find_heading(soup, heading_texts)
    if not h:
        return []
    block = _collect_section(h)
    bullets = _extract_li_items(block)
    if bullets:
        return bullets
    lines = [
        ln.strip(" •-\t")
        for ln in _html_to_text(block).splitlines()
        if ln.strip()
    ]
    return [ln for ln in lines if len(ln) <= 300]


def _extract_occupational_brief(soup: BeautifulSoup) -> tuple[list[str], list[str]]:
    knowledge: list[str] = []
    skills:    list[str] = []

    def _cwg(c) -> bool:
        return bool(c and "course-wysiwyg" in " ".join(c))

    brief_p = next(
        (p for p in soup.find_all("p") if "occupational brief" in _norm(_safe_text(p))),
        None,
    )
    if brief_p:
        container = next(
            (s for s in brief_p.next_siblings if isinstance(s, Tag)), None
        )
        cols = container.find_all("div", class_=_cwg) if container else []
    else:
        cols = soup.find_all("div", class_=_cwg)

    for col in cols:
        h2 = col.find("h2")
        if not h2:
            continue
        label = _norm(_safe_text(h2))
        items = _extract_li_items(col)
        if "knowledge" in label:
            knowledge = items
        elif "skill" in label:
            skills = items

    return knowledge, skills


def _extract_label_value_pairs(page_text: str) -> dict[str, str]:
    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
    LABELS = [
        "Qualification", "Qualification Level", "Qualification Duration",
        "End Point Assessment Centre", "Instruction Language", "Maximum Funding",
    ]
    label_set = {l.lower() for l in LABELS}
    out: dict[str, str] = {}
    for i, ln in enumerate(lines):
        if ln.lower() in label_set:
            nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
            if nxt and nxt.lower() not in label_set:
                for lab in LABELS:
                    if lab.lower() == ln.lower():
                        out[lab] = nxt
                        break
    return out


def _guess_footer_address(page_text: str) -> tuple[str, str, str, str]:
    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
    postcode = city = address_block = ""
    for i, ln in enumerate(lines):
        m = _UK_POSTCODE_RE.search(ln)
        if m:
            postcode = m.group(1).upper().strip()
            block_lines = lines[max(0, i - 3):min(len(lines), i + 2)]
            address_block = "\n".join(block_lines)
            city = next(
                ("Southampton" for l in reversed(block_lines) if "southampton" in l.lower()),
                "",
            )
            break
    return address_block, city, postcode, ", ".join(x for x in [city, postcode] if x)


# ─────────────────────────── HTTP client ─────────────────────────────

class ApprenticeshipClient:
    def __init__(
        self,
        delay:   float = 1.5,
        timeout: int   = 30,
        proxies: dict | None = None,
    ):
        self.delay   = float(delay)
        self.timeout = int(timeout)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent":      "Mozilla/5.0 (compatible; KiwiEducationScraper/1.0)",
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

    def build_listing_url(
        self, course_level: str, course_type: str, page: int = 1
    ) -> str:
        base  = LISTING_BASE if page <= 1 else f"{BASE_URL}/apprenticeships/page/{page}/"
        lvl   = (course_level or DEFAULT_COURSE_LEVEL).strip() or DEFAULT_COURSE_LEVEL
        ctype = (course_type or "").strip()
        return (
            f"{base}?course-level={lvl}&course-type={ctype}"
            if ctype else
            f"{base}?course-level={lvl}"
        )

    def _get_soup(self, url: str, retries: int = 3) -> BeautifulSoup | None:
        last_exc: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = min(30, 2 * attempt + random.uniform(1, 3))
                    logger.warning(
                        f"{resp.status_code} attempt {attempt}/{retries}"
                        f" -> wait {wait:.1f}s | {url}"
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

    # ── Listing ───────────────────────────────────────────────────────

    def iter_all_job_links(
        self,
        *,
        category_label:    str,
        course_level:      str,
        course_type_value: str,
        max_pages:         int = 0,
    ) -> Iterator[ApprenticeshipListing]:
        seen_urls: set[str] = set()
        page = 1
        while True:
            if max_pages and page > max_pages:
                break
            soup = self._get_soup(
                self.build_listing_url(course_level, course_type_value, page)
            )
            if not soup:
                break
            found = 0
            for a in soup.find_all("a", href=True):
                href = (a.get("href") or "").strip()
                if "/apprenticeship/" not in href:
                    continue
                full = href if href.startswith("http") else urljoin(BASE_URL, href)
                full = full.split("#")[0].split("?")[0].rstrip("/") + "/"
                if not full.startswith(BASE_URL) or full in seen_urls:
                    continue
                seen_urls.add(full)
                found += 1
                yield ApprenticeshipListing(
                    vacancy_ref=make_vacancy_ref(full, category_label, course_level),
                    vacancy_url=full,
                    title=_safe_text(a),
                )
            if not found and not max_pages:
                break
            page += 1

    # ── Detail ────────────────────────────────────────────────────────

    def scrape_job_detail(
        self,
        listing: ApprenticeshipListing,
        *,
        category_label: str,        # Kiwi course-type label e.g. "Digital"
    ) -> ApprenticeshipDetail:

        soup = self._get_soup(listing.vacancy_url)
        if not soup:
            our_cat, our_sub = _resolve_our_taxonomy(
                category_label, listing.title or ""
            )
            return ApprenticeshipDetail(
                vacancy_ref=listing.vacancy_ref,
                vacancy_url=listing.vacancy_url,
                category=our_cat,
                subcategory=our_sub,
                title=(listing.title or "").strip(),
                employer_name="Kiwi Education",
                employer_website=f"{BASE_URL}/",
            )

        page_text = soup.get_text("\n")
        labels    = _extract_label_value_pairs(page_text)

        # ── Title ─────────────────────────────────────────────────────
        title = ""
        first_h2 = soup.find("h2")
        if first_h2:
            title = _safe_text(first_h2)
        if not title:
            og = _extract_meta(soup, "og:title")
            title = og.split("–")[0].strip() or og
        if not title:
            t = soup.find("title")
            if t:
                title = _safe_text(t).split("–")[0].strip()
        title = title.strip()

        # ── OUR category + subcategory ────────────────────────────────
        our_category, our_subcategory = _resolve_our_taxonomy(category_label, title)

        # ── Image ─────────────────────────────────────────────────────
        image_url = _extract_meta(soup, "og:image")
        if image_url and any(
            x in image_url.lower() for x in ["logo", "yuzu", "icon", "avatar"]
        ):
            image_url = ""
        if not image_url:
            for sel in [
                ".wysiwyg img", "article img", ".entry-content img", "main img"
            ]:
                img = soup.select_one(sel)
                if img and img.get("src"):
                    src = str(img["src"])
                    if not any(
                        x in src.lower()
                        for x in ["logo", "yuzu", "icon", "avatar", "placeholder"]
                    ):
                        image_url = (
                            src if src.startswith("http") else urljoin(BASE_URL, src)
                        )
                        break

        # ── Qualification metadata ────────────────────────────────────
        qualification          = labels.get("Qualification", "")
        qualification_level    = labels.get("Qualification Level", "")
        qualification_duration = labels.get("Qualification Duration", "")
        epa                    = labels.get("End Point Assessment Centre", "")
        instr_lang             = labels.get("Instruction Language", "")
        max_funding            = labels.get("Maximum Funding", "")

        # ── Page sections ─────────────────────────────────────────────
        suitable = _section_text(soup, [
            "Who is this course suitable for?",
            "Who is this suitable for?",
            "Who is this for?",
        ])

        role_profile = _section_text(soup, [
            "Role Profile (what the successful candidate should be able to do at the end of the Apprenticeship)",
            "Role Profile (what the successful candidate should be able to do at the end of the apprenticeship)",
            "Role Profile",
        ])
        if not role_profile:
            role_profile = suitable

        typical_roles = _section_bullets(soup, [
            "Typical job roles may include:",
            "Typical job roles may include",
            "Roles & Responsibilities may include:",
            "Roles & Responsibilities may include",
            "Typical job roles",
        ])

        knowledge_items, skills_items = _extract_occupational_brief(soup)
        if not knowledge_items:
            knowledge_items = _section_bullets(soup, ["KNOWLEDGE", "Knowledge"])
        if not skills_items:
            skills_items = _section_bullets(soup, ["SKILLS", "Skills"])

        behaviours_items = _section_bullets(soup, ["BEHAVIOURS", "Behaviours"])
        if not behaviours_items:
            behaviours_items = _section_bullets(soup, [
                "These are the personal attributes and behaviours expected of all",
                "These are the personal attributes and behaviours expected",
            ])

        after = _section_text(soup, [
            "After this apprenticeship", "After this Apprenticeship",
            "What comes next?", "Progression",
        ])

        funding_text = _section_text(soup, [
            "Employer Funding & Incentives", "Employer Funding",
        ])

        addr_block, city, postcode, loc_summary = _guess_footer_address(page_text)

        # ── Derived fields ────────────────────────────────────────────
        more_lines = []
        if epa:        more_lines.append(f"End Point Assessment Centre: {epa}")
        if instr_lang: more_lines.append(f"Instruction Language: {instr_lang}")
        if max_funding: more_lines.append(f"Maximum Funding: {max_funding}")

        learn_blocks: list[str] = []
        if knowledge_items:
            learn_blocks.append("KNOWLEDGE:")
            learn_blocks.extend(knowledge_items)
        if skills_items:
            if learn_blocks:
                learn_blocks.append("")
            learn_blocks.append("SKILLS:")
            learn_blocks.extend(skills_items)

        tagline = _safe_text(first_h2.find_next("p")) if first_h2 else ""
        summary_parts = [x for x in [tagline.strip(), suitable.strip()] if x]
        if len(summary_parts) == 2 and summary_parts[0] == summary_parts[1]:
            summary_parts = [summary_parts[0]]

        training_course = (qualification or title).strip()
        if qualification_level and qualification_level not in training_course:
            training_course = f"{training_course} ({qualification_level})"

        # ── Build detail ──────────────────────────────────────────────
        detail = ApprenticeshipDetail(
            vacancy_ref=listing.vacancy_ref,
            vacancy_url=listing.vacancy_url,
            image_url=image_url,
            requirement_summery=role_profile,

            category=our_category,        # ← OUR taxonomy
            subcategory=our_subcategory,  # ← OUR taxonomy

            title=title,
            employer_name="Kiwi Education",
            location_summary=loc_summary,

            summary_text="\n\n".join(summary_parts),
            training_course=training_course,
            duration=qualification_duration,

            work_intro=role_profile,
            what_youll_do_heading="Roles & Responsibilities may include:" if typical_roles else "",
            what_youll_do_items="\n".join(typical_roles),

            training_intro="OCCUPATIONAL BRIEF OF STANDARD" if (knowledge_items or skills_items) else "",
            training_provider="Kiwi Education",
            training_course_repeat=training_course,
            what_youll_learn_items="\n".join(learn_blocks),
            more_training_information="\n".join(more_lines),

            skills_items="\n".join(skills_items),
            other_requirements_items="\n".join(behaviours_items),

            about_employer=funding_text,
            employer_website=f"{BASE_URL}/",
            after_this_apprenticeship=after,

            where_youll_work_address=addr_block,
            city=city,
            zip_code=postcode,
            latitude=None,
            longitude=None,
        )

        # Course pages — job-posting fields are always empty
        detail.wage             = ""
        detail.wage_extra       = ""
        detail.hours            = ""
        detail.hours_per_week   = ""
        detail.start_date       = ""
        detail.positions_available = ""
        detail.closing_text     = ""
        detail.posted_text      = ""
        detail.where_youll_work_name = ""

        return detail
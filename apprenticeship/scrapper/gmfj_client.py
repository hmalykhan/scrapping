# apprenticeship/scrapper/gmfj_client.py

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Iterable, Dict
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE = "https://www.getmyfirstjob.co.uk"


@dataclass(frozen=True)
class ListedVacancy:
    vacancy_ref: str
    url: str
    title: str = ""
    employer_name: str = ""
    location_summary: str = ""
    wage: str = ""
    closing_text: str = ""


class GmfjApprenticeshipClient:

    def __init__(self, delay: float = 0.7, timeout: int = 30):
        self.delay = delay
        self.timeout = timeout

        self.sess = requests.Session()
        self.sess.headers.update({
            "User-Agent": "Mozilla/5.0 (GMFJScraper/1.0)"
        })

        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
        )
        self.sess.mount("https://", HTTPAdapter(max_retries=retry))

    def soup(self, url: str) -> BeautifulSoup:
        r = self.sess.get(url, timeout=self.timeout)
        r.raise_for_status()
        time.sleep(self.delay)
        return BeautifulSoup(r.text, "lxml")

    # ---------------- LISTING ----------------
    def build_search_url(self, keyword: str, page: int = 1) -> str:
        qs = urlencode({
            "search": keyword,
            "page": page
        })
        return f"{BASE}/Search?{qs}"

    def iter_all_vacancies(self, keyword: str) -> Iterable[ListedVacancy]:
        page = 1

        while True:
            url = self.build_search_url(keyword, page)
            soup = self.soup(url)

            cards = soup.select(".vacancy-card, .job, .search-result")

            if not cards:
                break

            for card in cards:
                a = card.select_one("a[href]")
                if not a:
                    continue

                href = a.get("href")
                job_url = urljoin(BASE, href)

                title = a.get_text(strip=True)

                employer = self._safe_text(card, ".employer")
                location = self._safe_text(card, ".location")
                wage = self._safe_text(card, ".salary")

                vacancy_ref = job_url.split("/")[-1]

                yield ListedVacancy(
                    vacancy_ref=vacancy_ref,
                    url=job_url,
                    title=title,
                    employer_name=employer,
                    location_summary=location,
                    wage=wage,
                )

            page += 1

    def _safe_text(self, node, selector):
        el = node.select_one(selector)
        return el.get_text(strip=True) if el else ""

    # ---------------- DETAILS ----------------
    def scrape_vacancy_detail(self, url: str) -> Dict[str, str]:
        soup = self.soup(url)

        def txt(selector):
            el = soup.select_one(selector)
            return el.get_text("\n", strip=True) if el else ""

        return {
            "title": txt("h1"),
            "summary_text": txt(".job-description, .description"),
            "employer_name": txt(".employer, .company"),
            "location_summary": txt(".location"),
            "wage": txt(".salary"),

            "training_course": txt(".course"),
            "duration": txt(".duration"),

            "about_employer": txt(".about-company"),
            "closing_text": txt(".closing-date"),

            # fallback fields
            "work_intro": txt(".description"),
        }
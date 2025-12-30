# apprenticeship/scrapper/ncs.py
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://www.findapprenticeship.service.gov.uk"
VACANCY_PATH_RE = re.compile(r"^/apprenticeship/(VAC\d+)\b", re.I)

PUNCT_ONLY_RE = re.compile(r"^[\s,.;:–—-]+$")
URL_RE = re.compile(r"(https?://[^\s)]+)", re.I)
DOMAINISH_RE = re.compile(r"^(?:https?://|www\.)\S+$", re.I)

UI_SKIP_LINES = {
    "skip to main content",
    "print",
    "contents",
    "summary",
    "work",
    "training",
    "requirements",
    "about this employer",
    "after this apprenticeship",
    "ask a question",
    "apply now",
    "account",
    "menu",
}


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def abs_url(href: str, base: str = BASE) -> str:
    return urljoin(base, href)


def cleanup_lines(text: str) -> str:
    lines = [ln.strip() for ln in (text or "").splitlines()]
    out: List[str] = []
    for ln in lines:
        if not ln:
            if out and out[-1] != "":
                out.append("")
            continue
        if PUNCT_ONLY_RE.match(ln):
            continue
        low = ln.lower().strip()
        if low in UI_SKIP_LINES:
            continue
        out.append(ln)
    return "\n".join(out).strip()


def _lines(node: Tag) -> List[str]:
    raw = node.get_text("\n", strip=True)
    out: List[str] = []
    for ln in raw.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        if PUNCT_ONLY_RE.match(ln):
            continue
        if ln.lower().strip() in UI_SKIP_LINES:
            continue
        out.append(ln)
    return out


def _find_after_label(lines: List[str], label: str) -> str:
    """
    Works when:
      - label on one line and value on next
      - label + value on same line
    """
    base = label.strip().rstrip(":").lower()
    for i, ln in enumerate(lines):
        t = clean(ln)
        low = t.lower()
        if low == base:
            return clean(lines[i + 1]) if i + 1 < len(lines) else ""
        if low.startswith(base + " "):
            return clean(t[len(label):])
    return ""


def _bullet_items(scope: Tag) -> List[str]:
    items: List[str] = []
    for li in scope.select("ul li"):
        t = clean(li.get_text(" ", strip=True))
        if t:
            items.append(t)
    return items


def _bullets_text(scope: Tag) -> str:
    return "\n".join(_bullet_items(scope)).strip()


def _find_h2(root: Tag, title: str) -> Optional[Tag]:
    wanted = title.strip().lower()
    for h2 in root.find_all("h2"):
        if clean(h2.get_text(" ", strip=True)).lower() == wanted:
            return h2
    return None


def _collect_until_next_h2(start_h2: Tag) -> Tag:
    tmp = BeautifulSoup("<div></div>", "lxml").div  # type: ignore
    for sib in start_h2.next_siblings:
        if isinstance(sib, Tag) and sib.name == "h2":
            break
        if isinstance(sib, Tag):
            tmp.append(sib)
    return tmp  # type: ignore


def _find_h3_in_scope(scope: Tag, title: str) -> Optional[Tag]:
    wanted = title.strip().lower()
    for h in scope.find_all(["h3", "h4"]):
        if clean(h.get_text(" ", strip=True)).lower() == wanted:
            return h
    return None


def _collect_until_next_h3_or_h2(start_h: Tag) -> Tag:
    tmp = BeautifulSoup("<div></div>", "lxml").div  # type: ignore
    for sib in start_h.next_siblings:
        if isinstance(sib, Tag) and sib.name in ("h2", "h3"):
            break
        if isinstance(sib, Tag):
            tmp.append(sib)
    return tmp  # type: ignore


# ---------------- URL + "details/dl" helpers ----------------
def _normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if u.lower().startswith("www."):
        return "https://" + u
    return u


def _looks_like_domain(u: str) -> bool:
    u = (u or "").strip()
    if not u or " " in u or "@" in u:
        return False
    return "." in u


def _first_link_or_domain_from_text(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    # try full URL regex first
    m = URL_RE.search(text)
    if m:
        return _normalize_url(m.group(1))
    # else scan tokens
    for token in re.split(r"[\s,]+", text):
        t = token.strip().strip("()[]{}<>.,;")
        if not t:
            continue
        if DOMAINISH_RE.match(t):
            return _normalize_url(t)
        if _looks_like_domain(t):
            if t.lower().startswith("http"):
                return _normalize_url(t)
            return _normalize_url("https://" + t)
    return ""


def _dl_value(scope: Tag, label: str) -> str:
    wanted = label.strip().lower().rstrip(":")
    for dt in scope.find_all("dt"):
        dt_text = clean(dt.get_text(" ", strip=True)).lower().rstrip(":")
        if dt_text != wanted:
            continue
        dd = dt.find_next_sibling("dd")
        if not dd:
            return ""
        for a in dd.select("a[href]"):
            href = (a.get("href") or "").strip()
            if href:
                return _normalize_url(href)
        return _first_link_or_domain_from_text(dd.get_text(" ", strip=True))
    return ""


def _details_block_text(scope: Tag, summary_label: str) -> str:
    wanted = summary_label.strip().lower().rstrip(":")
    for d in scope.find_all("details"):
        summ = d.find("summary")
        if not summ:
            continue
        s_txt = clean(summ.get_text(" ", strip=True)).lower().rstrip(":")
        if s_txt != wanted:
            continue
        body = d.select_one(".govuk-details__text") or d
        txt = body.get_text("\n", strip=True)
        # remove summary line if duplicated
        txt = txt.replace(summ.get_text(" ", strip=True), "")
        return cleanup_lines(txt)
    return ""


def _extract_block_after_label(lines: List[str], label: str) -> str:
    """
    Line fallback:
      match 'Label', 'Label:' or 'Label -', return following block (until a known stopper).
    """
    base = label.strip().lower().rstrip(":")
    stoppers = {
        "training provider",
        "training course",
        "what you'll learn",
        "training schedule",
        "more training information",
        "requirements",
        "about this employer",
        "after this apprenticeship",
        "ask a question",
        "company benefits",
        "employer website",
        "work",
        "where you'll work",
        "what you'll do at work",
    }

    for i, ln in enumerate(lines):
        low = clean(ln).lower().strip()
        low2 = low.rstrip(":")
        if low2 == base or low.startswith(base + ":") or low.startswith(base + " -"):
            # inline remainder
            inline = ""
            if ":" in ln:
                inline = ln.split(":", 1)[1].strip()
            if inline:
                return cleanup_lines(_first_link_or_domain_from_text(inline) or inline)

            block: List[str] = []
            j = i + 1
            while j < len(lines):
                nxt = clean(lines[j]).lower().strip()
                if nxt in stoppers:
                    break
                block.append(lines[j])
                j += 1
            return cleanup_lines("\n".join(block))
    return ""


def _first_external_href(scope: Tag) -> str:
    """
    Generic: return first external-ish href found in scope; skip gov.uk and this service.
    """
    for a in scope.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        href = _normalize_url(href)
        low = href.lower()
        if low.startswith("http") and ("gov.uk" in low or "findapprenticeship.service.gov.uk" in low):
            continue
        if low.startswith("http"):
            return href
    # fallback: scan visible text
    return _first_link_or_domain_from_text(scope.get_text(" ", strip=True))


# ---------------- Listing DTO ----------------
@dataclass(frozen=True)
class ListedVacancy:
    vacancy_ref: str
    url: str
    title: str = ""
    employer_name: str = ""
    location_summary: str = ""
    start_date: str = ""
    training_course: str = ""
    wage: str = ""
    closing_text: str = ""
    posted_text: str = ""


class NcsApprenticeshipClient:
    """
    Find an apprenticeship scraper:
      - iter_all_vacancies(start_url=...) yields vacancy refs + URLs
      - scrape_vacancy_detail(url) returns dict of all fields from the vacancy page sections
    """

    def __init__(self, *, delay: float = 0.7, timeout: int = 30) -> None:
        self.delay = delay
        self.timeout = timeout

        self.sess = requests.Session()
        self.sess.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DjangoScraper/1.0)"})

        retry = Retry(
            total=5,
            backoff_factor=0.6,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        self.sess.mount("https://", HTTPAdapter(max_retries=retry))
        self.sess.mount("http://", HTTPAdapter(max_retries=retry))

    def soup(self, url: str) -> BeautifulSoup:
        r = self.sess.get(url, timeout=self.timeout)
        r.raise_for_status()
        time.sleep(self.delay)
        return BeautifulSoup(r.text, "lxml")

    # ---------------- Pagination ----------------
    def iter_pages(self, start_url: str) -> Iterable[Tuple[str, BeautifulSoup]]:
        seen = set()
        url = start_url
        while url and url not in seen:
            seen.add(url)
            s = self.soup(url)
            yield url, s
            url = self._next_url(s, current=url)

    def _next_url(self, soup: BeautifulSoup, *, current: str) -> Optional[str]:
        main = soup.find("main") or soup
        rel_next = main.select_one('a[rel="next"][href]')
        if rel_next:
            return abs_url(rel_next["href"], base=current)

        for a in main.select("a[href]"):
            if clean(a.get_text()).lower().startswith("next"):
                return abs_url(a["href"], base=current)
        return None

    # ---------------- Listing ----------------
    def iter_all_vacancies(self, *, start_url: str) -> Iterable[ListedVacancy]:
        for _, page in self.iter_pages(start_url):
            for v in self._extract_vacancies_from_list(page):
                yield v

    def _vac_refs_in_node(self, node: Tag) -> set[str]:
        refs: set[str] = set()
        for a in node.select('a[href^="/apprenticeship/"]'):
            href = a.get("href") or ""
            m = VACANCY_PATH_RE.match(href)
            if m:
                refs.add(m.group(1).upper())
        return refs

    def _find_result_card(self, link: Tag, *, vacancy_ref: str) -> Optional[Tag]:
        node: Optional[Tag] = link
        for _ in range(15):
            if not isinstance(node, Tag):
                return None

            refs = self._vac_refs_in_node(node)
            if refs and refs == {vacancy_ref}:
                txt = node.get_text(" ", strip=True)
                hits = sum(1 for lab in ("Start date", "Training course", "Wage", "Closes", "Posted") if lab in txt)
                if hits >= 2 or node.name in ("article", "li", "section", "div"):
                    return node

            node = node.parent if isinstance(node.parent, Tag) else None
        return None

    def _extract_vacancies_from_list(self, soup: BeautifulSoup) -> List[ListedVacancy]:
        main = soup.find("main") or soup
        out: List[ListedVacancy] = []

        for a in main.select('a[href^="/apprenticeship/"]'):
            href = a.get("href") or ""
            m = VACANCY_PATH_RE.match(href)
            if not m:
                continue

            vacancy_ref = m.group(1).upper()
            url = abs_url(href)
            title = clean(a.get_text(" ", strip=True))

            card = self._find_result_card(a, vacancy_ref=vacancy_ref)
            if not card:
                continue

            lines = _lines(card)

            employer_name = ""
            location_summary = ""
            if title and title in lines:
                i = lines.index(title)
                if i + 1 < len(lines):
                    employer_name = lines[i + 1]
                if i + 2 < len(lines):
                    location_summary = lines[i + 2]

            start_date = _find_after_label(lines, "Start date")
            training_course = _find_after_label(lines, "Training course")
            wage = _find_after_label(lines, "Wage")

            closing_text = ""
            posted_text = ""
            for ln in lines:
                low = ln.lower().strip()
                if low.startswith("closes"):
                    closing_text = ln
                elif low.startswith("posted"):
                    posted_text = ln

            out.append(
                ListedVacancy(
                    vacancy_ref=vacancy_ref,
                    url=url,
                    title=title,
                    employer_name=employer_name,
                    location_summary=location_summary,
                    start_date=start_date,
                    training_course=training_course,
                    wage=wage,
                    closing_text=closing_text,
                    posted_text=posted_text,
                )
            )

        seen = set()
        dedup: List[ListedVacancy] = []
        for v in out:
            if v.vacancy_ref not in seen:
                seen.add(v.vacancy_ref)
                dedup.append(v)
        return dedup

    # ---------------- Details ----------------
    def scrape_vacancy_detail(self, vacancy_url: str) -> Dict[str, str]:
        s = self.soup(vacancy_url)
        main = s.find("main") or s

        # Header/top area
        h1 = main.find("h1")
        title = clean(h1.get_text(" ", strip=True)) if h1 else ""

        all_lines = [clean(x) for x in main.get_text("\n", strip=True).splitlines() if clean(x)]
        employer_name = ""
        location_summary = ""
        closing_text = ""
        posted_text = ""

        if title and title in all_lines:
            i = all_lines.index(title)
            if i + 1 < len(all_lines):
                employer_name = all_lines[i + 1]
            if i + 2 < len(all_lines):
                location_summary = all_lines[i + 2]
            if i + 3 < len(all_lines) and all_lines[i + 3].lower().startswith("closes"):
                closing_text = all_lines[i + 3]
            for j in range(i + 1, min(i + 12, len(all_lines))):
                if all_lines[j].lower().startswith("posted"):
                    posted_text = all_lines[j]
                    break

        # Summary
        summary_text = ""
        wage = ""
        wage_extra = ""
        training_course = ""
        hours = ""
        hours_per_week = ""
        start_date = ""
        duration = ""
        positions_available = ""

        summary_h2 = _find_h2(main, "Summary")
        if summary_h2:
            scope = _collect_until_next_h2(summary_h2)
            lines = _lines(scope)

            wage = _find_after_label(lines, "Wage")
            training_course = _find_after_label(lines, "Training course")
            hours = _find_after_label(lines, "Hours")
            start_date = _find_after_label(lines, "Start date")
            duration = _find_after_label(lines, "Duration")
            positions_available = _find_after_label(lines, "Positions available")

            for ln in lines:
                if "hours a week" in ln.lower():
                    hours_per_week = ln.strip()
                    break

            if "Wage" in lines:
                idx = lines.index("Wage")
                summary_text = cleanup_lines("\n".join(lines[:idx]))

            if "Wage" in lines and "Training course" in lines:
                try:
                    w_i = lines.index("Wage")
                    end_i = lines.index("Training course")
                    start_i = min(w_i + 2, len(lines))
                    extra = []
                    for ln in lines[start_i:end_i]:
                        if "check minimum wage rates" in ln.lower():
                            continue
                        extra.append(ln)
                    wage_extra = cleanup_lines("\n".join(extra))
                except Exception:
                    pass

        # Work (robust intro + heading + items)
        work_intro = ""
        what_youll_do_heading = ""
        what_youll_do_items = ""
        where_youll_work_name = ""
        where_youll_work_address = ""

        work_h2 = _find_h2(main, "Work")
        if work_h2:
            work_scope = _collect_until_next_h2(work_h2)
            work_lines = _lines(work_scope)

            idx_do = next((i for i, ln in enumerate(work_lines) if ln.lower() == "what you'll do at work"), None)
            if idx_do is not None:
                work_intro = cleanup_lines("\n".join(work_lines[:idx_do]))
            else:
                work_intro = cleanup_lines("\n".join(work_lines))

            h3_do = _find_h3_in_scope(work_scope, "What you'll do at work")
            do_scope = _collect_until_next_h3_or_h2(h3_do) if h3_do else work_scope

            do_items_list = _bullet_items(do_scope)
            what_youll_do_items = "\n".join(do_items_list).strip()

            do_lines = _lines(do_scope)
            if do_lines and do_lines[0].lower() == "what you'll do at work":
                do_lines = do_lines[1:]
            if do_lines:
                cand = do_lines[0]
                if cand and (not do_items_list or cand != do_items_list[0]) and len(cand) <= 200:
                    if cand.lower() not in ("where you'll work", "work"):
                        what_youll_do_heading = cand

            h3_where = _find_h3_in_scope(work_scope, "Where you'll work")
            if h3_where:
                where_scope = _collect_until_next_h3_or_h2(h3_where)
                where_lines = [clean(x) for x in where_scope.get_text("\n", strip=True).splitlines() if clean(x)]
                if where_lines:
                    where_youll_work_name = where_lines[0]
                    where_youll_work_address = "\n".join(where_lines[1:]).strip()
            else:
                idx_where = next((i for i, ln in enumerate(work_lines) if ln.lower() == "where you'll work"), None)
                if idx_where is not None and idx_where + 1 < len(work_lines):
                    where_youll_work_name = work_lines[idx_where + 1]
                    where_youll_work_address = "\n".join(work_lines[idx_where + 2 :]).strip()

        # Training (✅ robust "More training information")
        training_intro = ""
        training_provider = ""
        training_course_repeat = ""
        what_youll_learn_items = ""
        training_schedule = ""
        more_training_information = ""

        training_h2 = _find_h2(main, "Training")
        if training_h2:
            t_scope = _collect_until_next_h2(training_h2)
            t_lines = _lines(t_scope)

            training_intro = cleanup_lines(t_scope.get_text("\n", strip=True))

            h3_provider = _find_h3_in_scope(t_scope, "Training provider")
            if h3_provider:
                ps = _collect_until_next_h3_or_h2(h3_provider)
                training_provider = clean(ps.get_text(" ", strip=True))

            h3_course = _find_h3_in_scope(t_scope, "Training course")
            if h3_course:
                cs = _collect_until_next_h3_or_h2(h3_course)
                c_lines = [clean(x) for x in cs.get_text("\n", strip=True).splitlines() if clean(x)]
                training_course_repeat = c_lines[0] if c_lines else ""

            h3_learn = _find_h3_in_scope(t_scope, "What you'll learn")
            if h3_learn:
                ls = _collect_until_next_h3_or_h2(h3_learn)
                what_youll_learn_items = _bullets_text(ls)

            h3_sched = _find_h3_in_scope(t_scope, "Training schedule")
            if h3_sched:
                ss = _collect_until_next_h3_or_h2(h3_sched)
                training_schedule = cleanup_lines(ss.get_text("\n", strip=True))

            # 1) <details> block
            more_training_information = _details_block_text(t_scope, "More training information")

            # 2) heading block
            if not more_training_information:
                h3_more = _find_h3_in_scope(t_scope, "More training information")
                if h3_more:
                    ms = _collect_until_next_h3_or_h2(h3_more)
                    more_training_information = cleanup_lines(ms.get_text("\n", strip=True))

            # 3) line fallback
            if not more_training_information:
                more_training_information = _extract_block_after_label(t_lines, "More training information")

        # Requirements
        essential_qualifications = ""
        skills_items = ""
        other_requirements_items = ""

        req_h2 = _find_h2(main, "Requirements")
        if req_h2:
            r_scope = _collect_until_next_h2(req_h2)

            h3_ess = _find_h3_in_scope(r_scope, "Essential qualifications")
            if h3_ess:
                es = _collect_until_next_h3_or_h2(h3_ess)
                essential_qualifications = cleanup_lines(es.get_text("\n", strip=True))

            h3_skills = _find_h3_in_scope(r_scope, "Skills")
            if h3_skills:
                sk = _collect_until_next_h3_or_h2(h3_skills)
                skills_items = _bullets_text(sk)

            h3_other = _find_h3_in_scope(r_scope, "Other requirements")
            if h3_other:
                ot = _collect_until_next_h3_or_h2(h3_other)
                other_requirements_items = _bullets_text(ot) or cleanup_lines(ot.get_text("\n", strip=True))

        # About employer (✅ robust Employer website + benefits)
        about_employer = ""
        employer_website = ""
        company_benefits_items = ""

        about_h2 = _find_h2(main, "About this employer")
        if about_h2:
            a_scope = _collect_until_next_h2(about_h2)
            a_lines = _lines(a_scope)

            about_employer = cleanup_lines(a_scope.get_text("\n", strip=True))

            # 1) <dl> pattern
            employer_website = _dl_value(a_scope, "Employer website")

            # 2) label/next lines pattern
            if not employer_website:
                tmp = _extract_block_after_label(a_lines, "Employer website")
                employer_website = _first_link_or_domain_from_text(tmp) or _normalize_url(tmp)

            # 3) anchor scan
            if not employer_website:
                employer_website = _first_external_href(a_scope)

            # Company benefits
            h3_ben = _find_h3_in_scope(a_scope, "Company benefits")
            if h3_ben:
                bs = _collect_until_next_h3_or_h2(h3_ben)
                company_benefits_items = _bullets_text(bs)
            else:
                company_benefits_items = _extract_block_after_label(a_lines, "Company benefits")

        # After
        after_this_apprenticeship = ""
        after_h2 = _find_h2(main, "After this apprenticeship")
        if after_h2:
            af = _collect_until_next_h2(after_h2)
            after_this_apprenticeship = _bullets_text(af) or cleanup_lines(af.get_text("\n", strip=True))

        # Ask a question
        contact_name = ""
        ask_h2 = _find_h2(main, "Ask a question")
        if ask_h2:
            ask_scope = _collect_until_next_h2(ask_h2)
            ask_lines = [clean(x) for x in ask_scope.get_text("\n", strip=True).splitlines() if clean(x)]
            for i, ln in enumerate(ask_lines):
                if ln.lower().startswith("the contact for this apprenticeship is"):
                    if i + 1 < len(ask_lines):
                        contact_name = ask_lines[i + 1]
                    break

        return {
            "title": title,
            "employer_name": employer_name,
            "location_summary": location_summary,
            "closing_text": closing_text,
            "posted_text": posted_text,

            "summary_text": summary_text,
            "wage": wage,
            "wage_extra": wage_extra,
            "training_course": training_course,
            "hours": hours,
            "hours_per_week": hours_per_week,
            "start_date": start_date,
            "duration": duration,
            "positions_available": positions_available,

            "work_intro": work_intro,
            "what_youll_do_heading": what_youll_do_heading,
            "what_youll_do_items": what_youll_do_items,
            "where_youll_work_name": where_youll_work_name,
            "where_youll_work_address": where_youll_work_address,

            "training_intro": training_intro,
            "training_provider": training_provider,
            "training_course_repeat": training_course_repeat,
            "what_youll_learn_items": what_youll_learn_items,
            "training_schedule": training_schedule,
            "more_training_information": more_training_information,

            "essential_qualifications": essential_qualifications,
            "skills_items": skills_items,
            "other_requirements_items": other_requirements_items,

            "about_employer": about_employer,
            "employer_website": employer_website,
            "company_benefits_items": company_benefits_items,

            "after_this_apprenticeship": after_this_apprenticeship,
            "contact_name": contact_name,
        }

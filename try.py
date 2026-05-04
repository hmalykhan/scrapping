# import requests
# from bs4 import BeautifulSoup
# url = "https://www.getmyfirstjob.co.uk/Search"
# res = requests.get(url)
# soup = BeautifulSoup(res.text, 'html.parser')
# jobs = soup.find_all("article")
# print(len(jobs))




from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://www.getmyfirstjob.co.uk/Search")
    page.wait_for_selector("article", state = "attached")
    jobs = page.query_selector_all("article")
    print("total jobs : ", len(jobs))
    browser.close()


# from playwright.sync_api import sync_playwright

# with sync_playwright() as p:
#     browser = p.chromium.launch(headless=True)
#     page = browser.new_page()

#     page.goto("https://www.getmyfirstjob.co.uk/Search")

#     # ✅ FIXED LINE
#     page.wait_for_selector("article", state="attached")

#     jobs = page.query_selector_all("article")

#     print("Total jobs:", len(jobs))

#     for job in jobs[:5]:
#         title = job.query_selector("h3").inner_text()
#         print(title)

#     browser.close()
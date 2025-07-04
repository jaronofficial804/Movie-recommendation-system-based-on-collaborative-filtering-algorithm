from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
driver = webdriver.Chrome(options=chrome_options)

url = "https://piaofang.maoyan.com/dashboard"
driver.get(url)
time.sleep(3)

data = []

rows = driver.find_elements(By.CSS_SELECTOR, 'table.dashboard-table tbody tr')[:10]

for idx, row in enumerate(rows, start=1):
    try:
        tds = row.find_elements(By.TAG_NAME, "td")
        name = tds[0].find_element(By.CSS_SELECTOR, ".moviename-name").text
        total_info = tds[0].find_elements(By.CSS_SELECTOR, ".moviename-info span")
        days = total_info[0].text if len(total_info) > 1 else ""
        total = total_info[-1].text if total_info else ""
        percent = tds[2].text
        shows = tds[3].text
        data.append({
            "rank": idx,
            "name": name,
            "total": total,
            "days": days,
            "percent": percent,
            "shows": shows
        })
    except Exception as e:
        print(f"Error in row {idx}: {e}")

driver.quit()

# 只保存需要的字段，不保存realtime
pd.DataFrame(data).to_csv("猫眼票房榜top10.csv", index=False, encoding="utf-8-sig")

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time

# 1. 启动浏览器（无头模式可选）
chrome_options = Options()
chrome_options.add_argument("--headless")  # 可去掉这一行显示窗口调试
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
driver = webdriver.Chrome(options=chrome_options)

url = "https://www.maoyan.com/news?showTab=1"
driver.get(url)
time.sleep(3)  # 必须，等内容全部渲染

# 2. 解析榜单内容
news_list = []

# Top1 特殊结构
try:
    top1 = driver.find_element(By.CSS_SELECTOR, ".top1-list")
    top1_title = top1.find_element(By.CSS_SELECTOR, ".top1-news-content a")
    news_list.append({
        "rank": 1,
        "title": top1_title.get_attribute("title") or top1_title.text,
        "link": "https://www.maoyan.com" + top1_title.get_attribute("href")
    })
except Exception as e:
    print("没有抓到榜首:", e)

# Top2-10
items = driver.find_elements(By.CSS_SELECTOR, "li .normal-link")
for idx, item in enumerate(items[:9], start=2):  # 只取前9个
    a = item.find_element(By.TAG_NAME, "a")
    news_list.append({
        "rank": idx,
        "title": a.get_attribute("title") or a.text,
        "link": "https://www.maoyan.com" + a.get_attribute("href")
    })

driver.quit()

# 打印/保存
for news in news_list:
    print(f"{news['rank']}. {news['title']}\n链接: {news['link']}\n")
pd.DataFrame(news_list).to_csv("猫眼热点榜top10.csv", index=False, encoding="utf-8-sig")

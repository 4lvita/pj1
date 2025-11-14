import pandas as pd
import chardet
import os
import json
import time
from playwright.sync_api import sync_playwright
from urllib.parse import urljoin

BASE_DIR = "./data"

DOWNLOAD_DIR = os.path.join(BASE_DIR, "attachments")

CSV_FILEPATH = os.path.join(BASE_DIR, "welfare_info_20250722.csv")

JSON_FILEPATH = os.path.join(BASE_DIR, "scraped_data.json")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

try:
    with open(CSV_FILEPATH, 'rb') as f:
        data = f.read(100000)
    result = chardet.detect(data)
    encoding = result['encoding']
    df = pd.read_csv(CSV_FILEPATH, encoding=encoding)
    print(f"CSV 파일 로드 성공. 총 {len(df)}개 서비스 발견")
except Exception as e:
    print(f"'{CSV_FILEPATH}' File Read Error {e}")
    df = pd.DataFrame()

def scrape_tabs(url: str, service_id: str, base_download_path: str) -> dict:
    service_data = {
        "url": url,
        "지원대상": "",
        "서비스_내용": "",
        "신청방법": "",
        "추가정보": "",
        "files":[]
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(url, timeout=5000)
            page.wait_for_load_state('domcontentloaded')

            tabs = {
                "지원대상": page.locator("#uuid-iw"),
                "서비스 내용": page.locator("#uuid-ix"),
                "신청방법": page.locator("#uuid-iy"),
                "추가정보": page.locator("#uuid-iz")
            }

            content_pane_selector = 'div[data-ndid="ij"] > div[role="tabpanel"]:not([style*="display: none"])'

            for tab_name, tab_locator in tabs.items():
                try:
                    if not tab_locator.count():
                        if not tab_locator.count():
                            print(f"  > [{tab_name}] 탭을 찾을 수 없어 건너뜁니다.")
                        continue
                        
                    tab_locator.click()
                    page.wait_for_timeout(1000)
                    
                    visible_pane = page.locator(content_pane_selector)
                    if visible_pane.count():
                        tab_text = visible_pane.inner_text()
                        service_data[tab_name] = tab_text.strip()
                    else:
                        print(f"  > [{tab_name}] 탭의 컨텐츠 영역을 찾을 수 없습니다.")


                    if tab_name == "추가정보":
                        download_buttons = visible_pane.locator('a[aria-label*="파일다운로드"]')
                        
                        button_count = download_buttons.count()
                        
                        for i in range(button_count):
                            button = download_buttons.nth(i)
                            
                            with page.expect_download() as download_info:
                                button.click()
                            
                            download = download_info.value
                            original_filename = download.suggested_filename
                            
                            new_filename = f"{service_id}_{original_filename}"
                            save_path = os.path.join(base_download_path, new_filename)
                            
                            download.save_as(save_path)

                            service_data["files"].append(save_path)

                except Exception as e:
                    print(f"  > [{tab_name}] 탭 처리 중 오류: {e}")
        
        except Exception as e:
            print(f"페이지 로드/처리 중 오류: {e}")
        
        finally:
            browser.close()
    
    return data

if not df.empty:

    all_scraped_data = []

    for index, row in df.iterrows():
        if pd.isna(row['서비스URL']) or pd.isna(row['서비스아이디']):
            continue

        url = row['서비스URL']
        service_id = str(row['서비스아이디'])
        service_name = str(row['서비스명'])

        print(f"\n--- {index + 1}/{len(df)} | {service_id} ({service_name}) 크롤링 시작 ---")
        print(f"URL: {url}")        

        try:
            scraped_data = scrape_tabs(url, service_id, DOWNLOAD_DIR)

            scraped_data['service_id'] = service_id
            scraped_data['service_name'] = service_name
            scraped_data['summary'] = str(row['서비스요약'])
            scraped_data['department'] = str(row['소관부처명'])

            all_scraped_data.append(scraped_data)

            time.sleep(0.5)

        except Exception as e:
            print(f"{service_id} 처리 중 심각한 오류 발생: {e}")

    print("\n--- 모든 크롤링 완료 ---")


    try:
        with open(JSON_FILEPATH, 'w', ecoding='utf-8') as f:
            json.dump(all_scraped_data, f, ensure_ascii=False, indent=4)

    except Exception as e:
        print(f"JSON FILE ERROR : {e}")

else:
    print("CSV 파일이 비어있거나 로드에 실패했습니다.") 
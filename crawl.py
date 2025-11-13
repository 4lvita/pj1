import pandas as pd
import chardet
from playwright.sync_api import sync_playwright
from urllib.parse import urljoin
import os

DOWNLOAD_DIR = "./data"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

filepath = os.path.join(DOWNLOAD_DIR, 'welfare_info_20250722.csv')

with open(filepath, 'rb') as f:
    data = f.read(100000)

result = chardet.detect(data)
encoding = result['encoding']

try:
    df = pd.read_csv(filepath, encoding=encoding)
except Exception as e:
    print(f"'{filepath}' File Read Error {e}")
    exit()

def scrape_tabs(url: str) -> dict:
    data = {
        "url": url,
        "지원대상":"",
        "서비스_내용":"",
        "신청방법":"",
        "추가정보":"",
        "files":[]
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            page.goto(url, timeout=10000)
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
                    print(f"[{tab_name}] 탭 클릭 중...")
                    tab_locator.click()
                    page.wait_for_timeout(1000) 
                    
                    visible_pane = page.locator(content_pane_selector)
                    tab_text = visible_pane.inner_text()
                    data[tab_name] = tab_text.strip()

                    # --- 3. 여기가 핵심 수정 부분입니다 ---
                    if tab_name == "추가정보":
                        # HTML 구조에 맞는 더 정확한 선택자 사용
                        # 'aria-label'에 "파일다운로드"가 포함된 <a> 태그를 찾습니다.
                        download_buttons = visible_pane.locator('a[aria-label*="파일다운로드"]')
                        
                        button_count = download_buttons.count()
                        if button_count > 0:
                            print(f"  > {button_count}개의 다운로드 버튼 발견.")

                        for i in range(button_count):
                            button = download_buttons.nth(i)
                            
                            # 파일명 로깅 (title 속성에서 가져오기)
                            filename_hint = button.get_attribute("title")
                            print(f"  > '{filename_hint}' 파일 다운로드 시도...")

                            # --- Playwright 다운로드 처리 ---
                            # 1. 다운로드 이벤트를 기다리는 리스너를 먼저 설정
                            with page.expect_download() as download_info:
                                # 2. 다운로드를 트리거하는 버튼 클릭
                                button.click()
                            
                            download = download_info.value
                            
                            # 3. 다운로드된 파일 저장
                            # (suggested_filename은 실제 다운로드되는 파일명입니다)
                            save_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
                            download.save_as(save_path)
                            
                            print(f"  > 파일 저장 완료: {save_path}")
                            data["files"].append(save_path) # 로컬 경로 저장
                                
                except Exception as e:
                    print(f"  > [{tab_name}] 탭 처리 중 오류: {e}")

        except Exception as e:
            print(f"페이지 로드/처리 중 심각한 오류: {e}")
        finally:
            browser.close()
            
    return data

if not df.empty:
    url = df.iloc[0]['서비스URL']
    test_url = "https://www.bokjiro.go.kr/ssis-tbu/twataa/wlfareInfo/moveTWAT52011M.do?wlfareInfoId=WLF00003170"
    print(f"테스트 크롤링 시작: {test_url}")
    
    scraped_data = scrape_tabs(test_url)
    
    print("\n--- 크롤링 결과 ---")
    print(f"URL: {scraped_data['url']}")
    print(f"\n[지원대상]:\n{scraped_data['지원대상'][:150]}...")
    print(f"\n[서비스 내용]:\n{scraped_data['서비스 내용'][:150]}...")
    print(f"\n[신청방법]:\n{scraped_data['신청방법'][:150]}...")
    print(f"\n[추가정보]:\n{scraped_data['추가정보'][:150]}...")
    print(f"\n[첨부파일]: {scraped_data['files']}")
else:
    print("CSV 파일이 비어있거나 로드에 실패했습니다.") 
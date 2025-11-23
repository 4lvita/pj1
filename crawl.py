import pandas as pd
import chardet
import asyncio
import os
import json
from playwright.async_api import async_playwright
from urllib.parse import urljoin

# --- 1. 설정 및 경로 ---
BASE_DATA_DIR = "./data"
DOWNLOAD_DIR = os.path.join(BASE_DATA_DIR, "attachments")
CSV_FILEPATH = os.path.join(BASE_DATA_DIR, 'welfare_info_20250722.csv')
JSON_SAVE_PATH = os.path.join(BASE_DATA_DIR, "bokjiro_scraped_data.json")

TIMEOUT_MS = 5000 
CONCURRENCY_LIMIT = 5
BATCH_SIZE = 10  # 데이터를 저장하는 주기 (10개씩 처리하고 저장)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- 2. CSV 파일 읽기 ---
print("데이터 파일 로딩 중...")
try:
    with open(CSV_FILEPATH, 'rb') as f:
        data = f.read(100000)
    result = chardet.detect(data)
    encoding = result['encoding']
    df = pd.read_csv(CSV_FILEPATH, encoding=encoding)
    print(f"CSV 로드 완료. 총 {len(df)}개의 서비스가 있습니다.")
except Exception as e:
    print(f"파일 읽기 실패: {e}")
    exit()

# --- 3. 기존 데이터 로드 (이어하기 기능) ---
def load_existing_data():
    if os.path.exists(JSON_SAVE_PATH):
        try:
            with open(JSON_SAVE_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # 중복 체크를 위해 완료된 ID들을 Set으로 만듦 (검색 속도 O(1))
                finished_ids = {item['service_id'] for item in data}
                print(f"🔄 기존 데이터 파일 발견: {len(data)}건이 이미 완료되었습니다.")
                return data, finished_ids
        except Exception as e:
            print(f"⚠️ 기존 파일 읽기 오류 (새로 시작): {e}")
            return [], set()
    else:
        print("✨ 기존 데이터 파일이 없습니다. 새로 시작합니다.")
        return [], set()

# --- 4. 비동기 크롤링 함수 (단일) ---
async def scrape_single_service(context, sem, row, base_download_path):
    async with sem: 
        service_id = str(row['서비스아이디'])
        service_name = str(row['서비스명'])
        url = row['서비스URL']
        
        service_data = {
            "service_id": service_id,
            "service_name": service_name,
            "url": url,
            "summary": str(row['서비스요약']),
            "department": str(row['소관부처명']),
            "지원대상": "",
            "서비스 내용": "",
            "신청방법": "",
            "추가정보": "",
            "files": []
        }

        if pd.isna(url) or pd.isna(service_id):
            return None

        page = await context.new_page()
        
        try:
            # 페이지 이동
            try:
                await page.goto(url, timeout=10000)
                await page.wait_for_load_state('networkidle', timeout=10000)
            except Exception:
                # print(f"  ⚠️ [{service_id}] 페이지 로드 타임아웃/오류 -> 건너뜀")
                await page.close()
                return service_data # 부분 데이터라도 반환 (나중에 채울 수도 있으므로)

            tabs = {
                "지원대상": page.locator(".custom-tabfolder .tabfolder-item").filter(has_text="지원대상"),
                "서비스 내용": page.locator(".custom-tabfolder .tabfolder-item").filter(has_text="서비스 내용"),
                "신청방법": page.locator(".custom-tabfolder .tabfolder-item").filter(has_text="신청방법"),
                "추가정보": page.locator(".custom-tabfolder .tabfolder-item").filter(has_text="추가정보")
            }
            
            content_pane_selector = '.cl-tabfolder-body > div[role="tabpanel"]:visible'
            
            for tab_name, tab_locator in tabs.items():
                try:
                    if await tab_locator.count() == 0:
                        continue
                        
                    await tab_locator.click(timeout=TIMEOUT_MS)
                    await page.wait_for_timeout(1000)
                    
                    visible_pane = page.locator(content_pane_selector)
                    
                    try:
                        await visible_pane.wait_for(state="visible", timeout=TIMEOUT_MS)
                        tab_text = await visible_pane.inner_text()
                        service_data[tab_name] = tab_text.strip()
                    except Exception:
                        continue

                    if tab_name == "추가정보":
                        download_buttons = visible_pane.locator('a[aria-label*="파일다운로드"]')
                        count = await download_buttons.count()
                        
                        for i in range(count):
                            button = download_buttons.nth(i)
                            try:
                                async with page.expect_download(timeout=TIMEOUT_MS) as download_info:
                                    await button.click(timeout=TIMEOUT_MS)
                                
                                download = await download_info.value
                                original_filename = download.suggested_filename
                                new_filename = f"{service_id}_{original_filename}"
                                save_path = os.path.join(base_download_path, new_filename)
                                
                                # 이미 파일이 있으면 다운로드 스킵 (선택사항)
                                if not os.path.exists(save_path):
                                    await download.save_as(save_path)
                                    print(f"    💾 저장: {new_filename}")
                                else:
                                    # print(f"    파일 이미 존재: {new_filename}")
                                    pass
                                    
                                service_data["files"].append(save_path)
                            except Exception:
                                pass
                                
                except Exception:
                    continue

            print(f"✅ 완료: {service_id} - {service_name}")
            
        except Exception as e:
            print(f"❌ 오류 발생 ({service_id}): {e}")
        finally:
            await page.close()
            
        return service_data

# --- 5. 메인 실행 함수 ---
async def main():
    # 1. 기존 데이터 로드
    all_results, finished_ids = load_existing_data()
    
    # 2. 아직 처리하지 않은 행만 필터링
    target_rows = []
    for index, row in df.iterrows():
        s_id = str(row['서비스아이디'])
        if s_id not in finished_ids:
            target_rows.append(row)
            
    total_target = len(target_rows)
    print(f"🚀 새로 처리할 데이터: {total_target}건")

    if total_target == 0:
        print("모든 데이터 처리가 완료되었습니다.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(accept_downloads=True)
        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)
        
        # 3. 배치 단위(BATCH_SIZE)로 나누어 실행 및 저장
        # 한 번에 다 돌리지 않고 끊어서 저장해야 중단 시 손실을 줄임
        for i in range(0, total_target, BATCH_SIZE):
            batch_rows = target_rows[i : i + BATCH_SIZE]
            tasks = []
            
            print(f"\n--- 배치 시작 ({i+1} ~ {min(i+BATCH_SIZE, total_target)} / {total_target}) ---")
            
            for row in batch_rows:
                task = scrape_single_service(context, sem, row, DOWNLOAD_DIR)
                tasks.append(task)
            
            # 배치 실행
            results = await asyncio.gather(*tasks)
            
            # 유효한 결과만 추가
            valid_batch_results = [r for r in results if r is not None]
            all_results.extend(valid_batch_results)
            
            # 4. 중간 저장 (핵심)
            try:
                with open(JSON_SAVE_PATH, 'w', encoding='utf-8') as f:
                    json.dump(all_results, f, ensure_ascii=False, indent=4)
                print(f"💾 현재까지 총 {len(all_results)}건 저장 완료.")
            except Exception as e:
                print(f"❌ 저장 중 오류 발생: {e}")

        await browser.close()
        print(f"\n🎉 모든 작업이 종료되었습니다. (최종 {len(all_results)}건)")

# --- 6. 실행 ---
if __name__ == "__main__":
    asyncio.run(main())
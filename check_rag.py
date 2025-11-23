import os
import sys
import shutil
import subprocess
import re

# ▼▼▼ [여기만 수정하세요] 테스트할 150MB짜리 파일 경로 ▼▼▼
TARGET_FILE_PATH = "./data/attachments/WLF00000896_2025년 정신건강사업 안내.hwp" 

def clean_text(text):
    if not text: return ""
    cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    return cleaned.replace('\u2028', '\n').replace('\u2029', '\n')

def test_hwp_extraction(filepath):
    print(f"🔍 파일 검사 시작: {filepath}")
    
    if not os.path.exists(filepath):
        print("❌ 파일이 존재하지 않습니다. 경로를 확인해주세요.")
        return

    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"📦 파일 크기: {file_size_mb:.2f} MB")

    hwp_cmd_path = shutil.which("hwp5txt")
    if hwp_cmd_path is None:
        print("❌ 'hwp5txt' 명령어를 찾을 수 없습니다.")
        return

    print("⏳ 텍스트 추출 중... (대용량이라 시간이 걸릴 수 있습니다)")
    
    try:
        # 파이썬으로 hwp5txt 실행
        cmd = [sys.executable, hwp_cmd_path, filepath]
        
        # 대용량 처리를 위해 timeout을 넉넉하게 3분(180초) 설정
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            encoding='utf-8',
            errors='ignore',
            timeout=180 
        )
        
        if result.returncode == 0:
            extracted_text = clean_text(result.stdout)
            text_len = len(extracted_text)
            
            if text_len < 100:
                print("⚠️ 경고: 추출된 텍스트가 너무 짧습니다. (이미지 위주 문서일 수 있음)")
                print(f"   - 추출된 내용: {extracted_text}")
            else:
                print(f"✅ 추출 성공! (글자 수: {text_len}자)")
                print(f"📜 내용 미리보기 (앞부분):\n{extracted_text[:200]}")
                print(f"\n📜 내용 미리보기 (뒷부분):\n{extracted_text[-200:]}")
        else:
            print(f"❌ 변환 실패 (Exit Code: {result.returncode})")
            print(f"   - 에러 로그: {result.stderr}")

    except subprocess.TimeoutExpired:
        print("❌ 시간 초과 (Timeout): 파일이 너무 커서 3분 안에 처리가 안 됩니다.")
    except Exception as e:
        print(f"❌ 실행 중 오류 발생: {e}")

if __name__ == "__main__":
    test_hwp_extraction(TARGET_FILE_PATH)
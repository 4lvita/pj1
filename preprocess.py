import os
import sys
import json
import hashlib
import shutil
import zipfile
import subprocess
import warnings
import tempfile

# --- 라이브러리 임포트 (설치 필요: pip install pymupdf easyocr pyhwp) ---
try:
    import fitz  # PyMuPDF
    import easyocr
except ImportError as e:
    print(f"❌ 필수 라이브러리가 없습니다. 설치해주세요: pip install pymupdf easyocr pyhwp")
    sys.exit(1)

warnings.filterwarnings("ignore")

# --- 설정 ---
BASE_DIR = "./data"
INPUT_JSON_PATH = os.path.join(BASE_DIR, "bokjiro_scraped_data.json")
FINAL_JSON_PATH = os.path.join(BASE_DIR, "bokjiro_rag_final.json")

# 중복 방지를 위한 글로벌 캐시 (Hash: Text)
CONTENT_CACHE = {}

# OCR 리더 초기화 (GPU 사용)
print("Preloading OCR Model...")
try:
    ocr_reader = easyocr.Reader(['ko', 'en'], gpu=True)
except Exception as e:
    print(f"⚠️ OCR 로딩 실패 (GPU 문제 가능성): {e}")
    ocr_reader = None

# --- 1. 유틸리티 함수 ---

def calculate_file_hash(filepath):
    """파일 내용의 SHA-256 해시 계산"""
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            # 메모리 효율을 위해 청크 단위로 읽기
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except:
        return None

# --- 2. 텍스트 추출 함수들 ---

def extract_text_from_hwp(filepath):
    """
    [수정됨] 'hwp5txt'를 sys.executable로 실행하여 권한 오류 방지 및 경로 자동 탐색
    """
    # 시스템 PATH에서 명령어 위치 찾기
    hwp_cmd_path = shutil.which("hwp5txt")
    
    if hwp_cmd_path is None:
        return "[Error] hwp5txt 명령어를 찾을 수 없습니다 (pip install pyhwp 필요)"

    try:
        # 파이썬 인터프리터를 통해 스크립트 실행 (Permission denied 해결 핵심)
        cmd = [sys.executable, hwp_cmd_path, filepath]
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            encoding='utf-8',
            errors='ignore' # 인코딩 에러 무시
        )
        
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            return "" # 변환 실패 시 빈 문자열 (암호 걸린 파일 등)
            
    except Exception as e:
        print(f"    ❌ HWP 실행 중 예외: {e}")
        return ""

def extract_text_from_pdf(filepath):
    """PDF 텍스트 추출"""
    text = ""
    try:
        doc = fitz.open(filepath)
        for page in doc:
            text += page.get_text()
        return text.strip()
    except Exception:
        return ""

def extract_text_from_image(filepath):
    """이미지 OCR"""
    if ocr_reader is None: return ""
    try:
        result = ocr_reader.readtext(filepath, detail=0)
        return " ".join(result)
    except Exception:
        return ""

def process_zip_recursive(zip_path):
    """ZIP 파일 재귀적 처리 및 텍스트 통합"""
    extracted_text_all = ""
    
    try:
        # 임시 폴더 생성하여 압축 해제
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    zf.extractall(temp_dir)
            except (zipfile.BadZipFile, RuntimeError):
                return "[Error] 손상되었거나 암호화된 ZIP 파일"

            # 해제된 파일 순회
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    full_path = os.path.join(root, file)
                    
                    # 재귀 호출 (중첩 ZIP) 또는 일반 파일 처리
                    # 주의: 무한 루프 방지를 위해 깊이 제한을 둘 수도 있음
                    file_text = process_file_router(full_path, is_inside_zip=True)
                    
                    if file_text:
                        extracted_text_all += f"\n[압축내용: {file}]\n{file_text}\n"
                        
    except Exception as e:
        print(f"    ❌ ZIP 처리 중 오류: {e}")
        
    return extracted_text_all

def process_file_router(filepath, is_inside_zip=False):
    """파일 확장자에 따른 처리 분기 및 중복 체크 (핵심 로직)"""
    if not os.path.exists(filepath):
        return ""

    # 1. 해시 계산 (중복 방지)
    # 압축 파일 내부의 파일은 경로가 임시적이라 해시 계산이 중요함
    file_hash = calculate_file_hash(filepath)
    
    # 캐시에 있으면 바로 반환
    if file_hash and file_hash in CONTENT_CACHE:
        # ZIP 내부 파일이 아닐 때만 로그 출력 (로그 폭탄 방지)
        if not is_inside_zip:
            print(f"    ⚡ 중복 파일 감지 (Skip): {os.path.basename(filepath)}")
        return CONTENT_CACHE[file_hash]

    # 2. 텍스트 추출
    ext = os.path.splitext(filepath)[1].lower()
    extracted_text = ""

    try:
        if ext == '.zip':
            if not is_inside_zip: # 최상위 ZIP만 로그 표시
                print(f"    📦 ZIP 해제 및 분석 중: {os.path.basename(filepath)}")
            extracted_text = process_zip_recursive(filepath)
            
        elif ext in ['.hwp', '.hwpx']:
            if not is_inside_zip: print(f"    📄 HWP 변환 중: {os.path.basename(filepath)}")
            extracted_text = extract_text_from_hwp(filepath)
            
        elif ext == '.pdf':
            if not is_inside_zip: print(f"    📄 PDF 변환 중: {os.path.basename(filepath)}")
            extracted_text = extract_text_from_pdf(filepath)
            
        elif ext in ['.jpg', '.jpeg', '.png', '.bmp']:
            if not is_inside_zip: print(f"    👁️ OCR 수행 중: {os.path.basename(filepath)}")
            extracted_text = extract_text_from_image(filepath)

    except Exception as e:
        print(f"    ❌ 처리 오류 ({os.path.basename(filepath)}): {e}")

    # 3. 결과 캐싱 (빈 텍스트라도 캐싱하여 재처리 방지)
    if file_hash:
        CONTENT_CACHE[file_hash] = extracted_text
        
    return extracted_text

# --- 3. 메인 실행 함수 ---

def main():
    print("=== [전처리] 복지 데이터 텍스트 통합 시작 ===")
    
    if not os.path.exists(INPUT_JSON_PATH):
        print(f"입력 파일이 없습니다: {INPUT_JSON_PATH}")
        print("먼저 크롤링 스크립트를 실행해주세요.")
        return

    # JSON 로드
    with open(INPUT_JSON_PATH, 'r', encoding='utf-8') as f:
        data_list = json.load(f)

    total_items = len(data_list)
    print(f"총 {total_items}개의 서비스 데이터를 처리합니다.")
    
    for idx, item in enumerate(data_list):
        service_id = item.get('service_id', 'Unknown')
        service_name = item.get('service_name', '')
        file_paths = item.get('files', [])
        
        print(f"\n[{idx+1}/{total_items}] {service_id} : {service_name}")
        
        all_attachment_text = ""
        
        # 해당 서비스의 첨부파일 처리
        if file_paths:
            for filepath in file_paths:
                text = process_file_router(filepath)
                if text and text.strip():
                    filename = os.path.basename(filepath)
                    all_attachment_text += f"\n\n=== [첨부파일: {filename}] ===\n{text}\n==========================\n"
        
        # --- [핵심] RAG를 위한 통합 텍스트 필드 생성 ---
        # 검색 엔진이 이 필드 하나만 읽으면 되도록 모든 정보를 때려 넣습니다.
        rag_text = f"""
[기본 정보]
서비스명: {service_name}
소관부처: {item.get('department', '')}
서비스요약: {item.get('summary', '')}

[지원 대상]
{item.get('지원대상', '내용 없음')}

[서비스 상세 내용]
{item.get('서비스 내용', '내용 없음')}

[신청 방법]
{item.get('신청방법', '내용 없음')}

[추가 정보]
{item.get('추가정보', '')}

[첨부파일 상세 내용]
{all_attachment_text if all_attachment_text else "첨부파일 내용 없음"}
        """.strip()
        
        # 결과 저장
        item['rag_full_text'] = rag_text
        
        # (선택사항) 너무 긴 로그 방지를 위해 파일별 추출 텍스트는 별도 필드로도 저장 가능
        # item['processed_attachments'] = all_attachment_text

    # 최종 결과 파일 저장
    print(f"\n💾 최종 결과 저장 중... ({FINAL_JSON_PATH})")
    with open(FINAL_JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(data_list, f, ensure_ascii=False, indent=4)
        
    print(f"✅ 모든 작업 완료! 처리된 고유 파일 수: {len(CONTENT_CACHE)}개")

if __name__ == "__main__":
    main()
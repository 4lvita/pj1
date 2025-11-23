import json
import os
import sys
import chromadb
from llama_index.core import Document, VectorStoreIndex, StorageContext, Settings
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.vector_stores.chroma import ChromaVectorStore

# --- 1. 설정 및 경로 ---
BASE_DIR = "./data"
INPUT_JSON_PATH = os.path.join(BASE_DIR, "bokjiro_rag_final.json") # 전처리 완료된 파일
DB_PATH = "./chroma_db"           # 벡터 DB가 저장될 로컬 폴더
COLLECTION_NAME = "welfare_policy" # DB 내부 컬렉션 이름

# --- 2. 임베딩 모델 설정 (WSL -> Windows Ollama 연결) ---
# 주의: Windows에서 'OLLAMA_HOST=0.0.0.0' 설정이 되어 있어야 합니다.
# host.docker.internal은 WSL에서 Windows localhost를 가리키는 주소입니다.
embed_model = OllamaEmbedding(
    model_name="mxbai-embed-large",
    base_url="http://host.docker.internal:11434", 
    ollama_additional_kwargs={"mirostat": 0},
    embed_batch_size=10 
)

# 전역 설정: 임베딩 모델 지정 / LLM은 생성 단계가 아니므로 None
Settings.embed_model = embed_model
Settings.llm = None 

# --- 3. 데이터 로딩 함수 ---
def load_documents_from_json():
    """전처리된 JSON 파일을 LlamaIndex Document 객체로 변환"""
    if not os.path.exists(INPUT_JSON_PATH):
        print(f"❌ 입력 파일이 없습니다: {INPUT_JSON_PATH}")
        print("먼저 'preprocess_final.py'를 실행하여 데이터를 준비해주세요.")
        sys.exit(1)

    with open(INPUT_JSON_PATH, 'r', encoding='utf-8') as f:
        data_list = json.load(f)

    documents = []
    print(f"📂 데이터 파일 로딩 중... (총 {len(data_list)}건)")

    for item in data_list:
        # [핵심] 전처리 단계에서 만든 '통합 텍스트 필드'를 사용합니다.
        text_content = item.get('rag_full_text', '')
        
        if not text_content.strip():
            continue 

        # 메타데이터 구성 (검색 후 출처 표기나 필터링에 사용)
        metadata = {
            "service_id": item.get('service_id', 'unknown'),
            "service_name": item.get('service_name', '제목 없음'),
            "department": item.get('department', ''),
            "url": item.get('url', '')
        }

        # Document 객체 생성
        doc = Document(
            text=text_content,
            metadata=metadata,
            # 임베딩(벡터 계산)할 때 제외할 메타데이터 키 설정
            # (URL이나 ID는 의미론적 유사성과 관계없으므로 제외하는 게 성능에 좋음)
            excluded_embed_metadata_keys=["url", "service_id", "service_name"] 
        )
        documents.append(doc)
    
    return documents

# --- 4. 인덱싱(벡터 저장) 실행 함수 ---
def build_index():
    # 1. 문서 변환
    documents = load_documents_from_json()
    if not documents:
        print("⚠️ 저장할 데이터가 없습니다.")
        return
        
    print(f"✅ 문서 변환 완료: {len(documents)}개의 Document 객체 준비됨.")

    # 2. ChromaDB 클라이언트 초기화
    print(f"💾 ChromaDB 초기화 중... (저장 경로: {os.path.abspath(DB_PATH)})")
    db_client = chromadb.PersistentClient(path=DB_PATH)
    
    # 컬렉션 생성 (기존에 있으면 불러옴)
    chroma_collection = db_client.get_or_create_collection(COLLECTION_NAME)

    # 3. Vector Store 설정
    vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    # 4. 인덱싱 (Embedding & Storing)
    print("🚀 벡터 변환 및 저장 시작... (데이터 양에 따라 시간이 소요됩니다)")
    
    # from_documents 함수가 임베딩 -> 벡터화 -> DB 저장을 자동으로 수행합니다.
    VectorStoreIndex.from_documents(
        documents,
        storage_context=storage_context,
        show_progress=True # 진행률 바 표시
    )
    
    print("\n🎉 [완료] 모든 데이터가 벡터 DB에 저장되었습니다!")
    print(f"   - DB 경로: {DB_PATH}")
    print(f"   - 컬렉션명: {COLLECTION_NAME}")

if __name__ == "__main__":
    # 실행 전 체크 사항 출력
    print("⚠️  [Check] 윈도우에서 'ollama serve'가 실행 중인가요?")
    print("⚠️  [Check] 'ollama pull mxbai-embed-large'를 하셨나요?")
    print("---------------------------------------------------------")
    
    try:
        build_index()
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        print("팁: Connection 오류라면 윈도우의 OLLAMA_HOST 환경변수나 방화벽을 확인하세요.")
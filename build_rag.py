import json
import os
import sys
import chromadb
from llama_index.core import Document, StorageContext, Settings
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.node_parser import SentenceSplitter

# --- 설정 ---
BASE_DIR = "./data"
INPUT_JSON_PATH = os.path.join(BASE_DIR, "bokjiro_rag_final.json")
DB_PATH = "./chroma_db"
COLLECTION_NAME = "welfare_policy"

# 병렬 처리 설정 (OLLAMA_NUM_PARALLEL 값과 맞춰주세요)
NUM_WORKERS = 4 

# --- 임베딩 모델 설정 ---
embed_model = OllamaEmbedding(
    model_name="mxbai-embed-large",
    base_url="http://host.docker.internal:11434", 
    ollama_additional_kwargs={"mirostat": 0},
    # 배치 사이즈를 늘려서 한 번에 많이 처리 (GPU 메모리 활용)
    embed_batch_size=32 
)

Settings.embed_model = embed_model
Settings.llm = None 

def load_documents_from_json():
    if not os.path.exists(INPUT_JSON_PATH):
        print(f"❌ 파일 없음: {INPUT_JSON_PATH}")
        sys.exit(1)

    with open(INPUT_JSON_PATH, 'r', encoding='utf-8') as f:
        data_list = json.load(f)

    documents = []
    print(f"📂 데이터 로딩 중... ({len(data_list)}건)")

    for item in data_list:
        text_content = item.get('rag_full_text', '')
        if not text_content.strip(): continue 

        metadata = {
            "service_id": item.get('service_id', 'unknown'),
            "service_name": item.get('service_name', '제목 없음'),
            "department": item.get('department', ''),
            "url": item.get('url', '')
        }

        doc = Document(
            text=text_content,
            metadata=metadata,
            excluded_embed_metadata_keys=["url", "service_id", "service_name"] 
        )
        documents.append(doc)
    
    return documents

def build_index_parallel():
    documents = load_documents_from_json()
    if not documents: return

    print(f"💾 ChromaDB 연결 중... ({DB_PATH})")
    db_client = chromadb.PersistentClient(path=DB_PATH)
    chroma_collection = db_client.get_or_create_collection(COLLECTION_NAME)
    vector_store = ChromaVectorStore(chroma_collection=chroma_collection)

    # --- [핵심] 병렬 처리를 위한 파이프라인 구축 ---
    print(f"🚀 병렬 인덱싱 파이프라인 시작 (Workers: {NUM_WORKERS})")
    
    pipeline = IngestionPipeline(
        transformations=[
            # 1. 문서를 청크(Chunk)로 자르기 (의미 단위 512 토큰)
            SentenceSplitter(chunk_size=512, chunk_overlap=50),
            # 2. 임베딩 (벡터 변환)
            embed_model,
        ],
        vector_store=vector_store, # 변환된 데이터를 바로 DB에 저장
    )

    # 병렬 실행
    # run 함수가 문서를 쪼개고 -> 임베딩하고 -> DB에 넣는 과정을 병렬로 수행합니다.
    pipeline.run(documents=documents, num_workers=NUM_WORKERS)
    
    print("\n🎉 [병렬 처리 완료] 모든 데이터가 저장되었습니다!")

if __name__ == "__main__":
    try:
        # 기존 DB가 있다면 충돌 방지를 위해 알림
        if os.path.exists(DB_PATH):
            print("ℹ️  기존 DB 폴더에 추가(Upsert)하거나 덮어씁니다.")
            
        build_index_parallel()
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
import requests
from bs4 import BeautifulSoup
from io import BytesIO
from PIL import Image
import pymysql

# --- 전역 설정 변수 ---
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'my_database'
IMAGE_DIR = r'C:\gitmain\scrap\images2' # Raw 문자열로 경로 지정
# URL 변경: 이제 페이지 번호는 loop에서 추가됩니다.

NUM_PAGES_TO_SCRAPE = 5 # 수집할 페이지 수
FILENAME_PATTERN = r'[\\/:"*?<>|]' # 파일명에 사용할 수 없는 문자 패턴
PAGE_SCRAPE_DELAY = 1 # 각 페이지 스크래핑 후 지연 시간 (초)




def load_news_from_db():
    """
    MySQL 데이터베이스의 NEWS 테이블에서 모든 뉴스 데이터를 불러와 반환합니다.
    """
    # 1. MySQL 데이터베이스 연결
    db_con = pymysql.connect(host=DB_HOST,
                             user=DB_USER,
                             password=DB_PASSWORD,
                             db=DB_NAME,
                             charset='utf8mb4')
    db_cursor = db_con.cursor(pymysql.cursors.DictCursor) # 컬럼 이름을 키로 하는 딕셔너리 형태로 데이터를 가져오기 위해 DictCursor 사용
    print("MySQL DB연결 성공.")

    # 2. NEWS 테이블에서 모든 데이터 조회
    select_query = "SELECT ID, TITLE, CONTENTS, NEWSDAY, IMAGE, COMPANY FROM NEWS"
    db_cursor.execute(select_query)
    
    # 3. 모든 데이터 가져오기
    news_data = db_cursor.fetchall()
    
    # 4. 데이터베이스 연결 닫기
    db_con.close()
    print("데이터베이스 연결이 닫혔습니다.")

    return news_data

if __name__ == "__main__":
    print("데이터베이스에서 뉴스 데이터를 불러오는 중...")
    loaded_news = load_news_from_db()

    if loaded_news:
        print(f"총 {len(loaded_news)}개의 뉴스 데이터를 불러왔습니다.")
        print("\n--- 불러온 데이터 미리보기 (상위 20개) ---")
        for i, news_item in enumerate(loaded_news[:20]):
            print(f"ID: {news_item['ID']}")
            print(f"제목: {news_item['TITLE']}")
            print(f"내용: {news_item['CONTENTS'][:50]}...") # 내용이 길 수 있으므로 앞부분만 출력
            print(f"날짜: {news_item['NEWSDAY']}")
            print(f"이미지 경로: {news_item['IMAGE']}")
            print(f"회사: {news_item['COMPANY']}")
            print("-" * 30)
    else:
        print("데이터베이스에서 뉴스 데이터를 찾을 수 없습니다.")


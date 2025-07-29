import pymysql
import csv
import os

# --- 데이터베이스 연결 설정 (기존 스크래핑 코드와 동일하게 설정) ---
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'my_database'

# --- CSV 파일 저장 경로 설정 ---
# 현재 스크립트가 실행되는 디렉토리에 저장됩니다.
# 필요하다면 다른 경로를 지정할 수 있습니다.
OUTPUT_DIR = '.' # 현재 디렉토리
CSV_FILENAME = 'news_data.csv'
CSV_FULL_PATH = os.path.join(OUTPUT_DIR, CSV_FILENAME)

# export_news_to_csv 함수를 제거하고, 그 내용을 직접 실행 블록에 배치합니다.
if __name__ == "__main__":
    # 1. MySQL 데이터베이스 연결
    db_con = pymysql.connect(host=DB_HOST,
                             user=DB_USER,
                             password=DB_PASSWORD,
                             db=DB_NAME,
                             charset='utf8mb4')
    db_cursor = db_con.cursor()
    print("MySQL 데이터베이스에 성공적으로 연결되었습니다.")

    # 2. NEWS 테이블에서 모든 데이터 조회
    select_query = "SELECT ID, TITLE, CONTENTS, NEWSDAY, IMAGE, COMPANY FROM NEWS"
    db_cursor.execute(select_query)
    
    # 컬럼 이름 가져오기 (CSV 헤더로 사용)
    column_names = [desc[0] for desc in db_cursor.description]
    
    # 3. CSV 파일로 저장
    with open(CSV_FULL_PATH, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        
        # 헤더(컬럼 이름) 쓰기
        csv_writer.writerow(column_names)
        
        # 데이터 행 쓰기
        for row in db_cursor:
            csv_writer.writerow(row)
    
    print(f"데이터가 성공적으로 '{CSV_FULL_PATH}' 파일로 내보내졌습니다.")

    # 4. 데이터베이스 연결 닫기
    db_con.close()
    print("데이터베이스 연결이 닫혔습니다.")

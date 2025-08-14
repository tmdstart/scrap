import mariadb
import sys

# 1. 원본 서버 연결 (192.168.111.111)
try:
    conn_src = mariadb.connect(
        user="lguplus7",
        password="lg7p@ssw0rd~!",
        host="192.168.40.38",
        port=3310,
        database="cp_data"
    )
except mariadb.Error as e:
    print(f"원본 서버 연결 오류: {e}")
    sys.exit(1)
src_cursor = conn_src.cursor()

# 2. 대상 서버 연결 (192.178.111.115)
try:
    conn_dest = mariadb.connect(
        user="lguplus7",
        password="lg7p@ssw0rd~!",
        host="localhost",
        port=3310,
        database="cp_data"
    )
except mariadb.Error as e:
    print(f"대상 서버 연결 오류: {e}")
    sys.exit(1)
dest_cursor = conn_dest.cursor()

try:
    # 3. 원본 서버에서 데이터 조회 (seq_no 컬럼은 제외)
    # 실제 컬럼명에 맞게 SELECT 문을 수정해야 합니다.
    src_cursor.execute("SELECT STN_ID, LON, ... FROM tb_weather_tcn")
    rows = src_cursor.fetchall()
    
    # 4. 대상 서버에 데이터 삽입
    # VALUES 문의 ? 개수를 SELECT 문 컬럼 개수와 맞춥니다.
    insert_sql = "INSERT INTO tb_weather_tcn (STN_ID, LON, ...) VALUES (?, ?, ...)"
    dest_cursor.executemany(insert_sql, rows)
    
    # 5. 변경 사항 커밋
    conn_dest.commit()
    print(f"{dest_cursor.rowcount}개의 행이 성공적으로 복사되었습니다.")
    
except mariadb.Error as e:
    print(f"데이터베이스 오류: {e}")
    conn_dest.rollback()
finally:
    src_cursor.close()
    conn_src.close()
    dest_cursor.close()
    conn_dest.close()


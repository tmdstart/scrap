import mariadb
import sys
import csv
import os

def migrate_data_no_pandas(file_path):
    """
    지정된 경로의 CSV 파일을 읽어 MariaDB 테이블에 삽입하는 함수입니다.
    
    Args:
        file_path (str): 삽입할 CSV 파일의 전체 경로 (예: 'C:\data\shop\Gangwon.csv').
    """

    data_to_insert = []
    try:
        # 파일이 CSV가 아닐 경우 함수를 종료합니다.
        if not file_path.endswith('.csv'):
            print(f"경고: CSV 파일이 아닙니다. 이 파일을 건너뜁니다: {file_path}")
            return
            
        print(f"\n--- {os.path.basename(file_path)} 파일 처리 시작 ---")

        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None) # 헤더(첫 번째 줄)를 건너뜁니다.
            
            for row in reader:
                processed_row = [value if len(str(value).strip()) > 0 else None for value in row]
                if len(processed_row) == 39: # 컬럼 수가 39개인지 확인
                    data_to_insert.append(processed_row)
                else:
                    print(f"경고: 컬럼 수가 일치하지 않아 이 행을 건너뜁니다: {row}")
        
        print(f"파일을 성공적으로 읽었습니다: {file_path}")
        print(f"총 {len(data_to_insert)}개의 유효한 행을 준비했습니다.")
        
    except FileNotFoundError:
        print(f"오류: 파일을 찾을 수 없습니다. 경로를 확인해주세요: {file_path}")
        return
    except Exception as e:
        print(f"파일 처리 중 오류가 발생했습니다: {e}")
        return
    
    try:
        conn = mariadb.connect(
            user="lguplus7",
            password="lg7p@ssw0rd~!",
            host="localhost",
            port=3310,
            database="cp_data"
        )
        cursor = conn.cursor()

        insert_sql = "INSERT INTO `tb_smb_ods` (`col1`, `col2`, `col3`, `col4`, `col5`, `col6`, `col7`, `col8`, `col9`, `col10`, `col11`, `col12`, `col13`, `col14`, `col15`, `col16`, `col17`, `col18`, `col19`, `col20`, `col21`, `col22`, `col23`, `col24`, `col25`, `col26`, `col27`, `col28`, `col29`, `col30`, `col31`, `col32`, `col33`, `col34`, `col35`, `col36`, `col37`, `col38`, `col39`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        # 데이터 삽입을 1000개씩 나누어서 처리
        chunk_size = 1000
        for i in range(0, len(data_to_insert), chunk_size):
            chunk = data_to_insert[i:i + chunk_size]
            cursor.executemany(insert_sql, chunk)
            conn.commit()
            print(f"[{i+1} ~ {i + len(chunk)}] 행 삽입 완료")

        print(f"\n--- {os.path.basename(file_path)} 파일 처리 종료 ---")
    
    except mariadb.Error as e:
        print(f"MariaDB 데이터베이스 오류: {e}")
        conn.rollback()
    except Exception as e:
        print(f"알 수 없는 오류가 발생했습니다: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# 스크립트 실행 부분: 이 부분만 수정되었습니다.
if __name__ == "__main__":
    base_path = r"C:\data\shop"
    
    # 지정된 경로에 있는 모든 파일 목록을 가져옵니다.
    for filename in os.listdir(base_path):
        full_path = os.path.join(base_path, filename)
        
        # 파일인지 확인하고, CSV 파일인 경우에만 함수를 호출합니다.
        if os.path.isfile(full_path) and filename.endswith('.csv'):
            migrate_data_no_pandas(full_path)
import sys
import mariadb
import datetime
import csv
import json

if __name__ == "__main__":
    connection = None
    cursor = None
    try:
        print("Connecting to the database...")
        # 데이터베이스 연결 정보를 직접 전달합니다.
        connection = mariadb.connect(
            user="lguplus7",
            password="lg7p@ssw0rd~!",
            host="localhost",
            port=3310,
            database="cp_data"
        )
        print("MariaDB connection successful.")

        # CSV 파일 경로와 이름을 설정합니다.
        # 이 코드를 사용하려면 엑셀 파일을 먼저 CSV 형식으로 저장해야 합니다.
        file_path = "C:\\data\\weather\\aws_data_202508121059.csv"

        # CSV 파일 데이터를 읽습니다.
        insert_data = []
        try:
            with open(file_path, 'r', newline='', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                # 헤더가 있는 경우, 다음 줄을 주석 해제하여 헤더를 건너뛸 수 있습니다.
                # next(csv_reader)
                
                for row in csv_reader:
                    if len(row) == 18:  # 엑셀 파일의 열 개수와 동일한지 확인합니다.
                        # 'update_dt'와 'org_data' 컬럼을 추가합니다.
                        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        original_data_json = json.dumps(row, ensure_ascii=False)
                        
                        # SQL 쿼리에 맞게 튜플 형식으로 데이터를 만듭니다.
                        record = tuple(row) + (original_data_json, current_time)
                        insert_data.append(record)
                    else:
                        print(f"Skipping a row with an incorrect number of columns: {row}")

            print(f"CSV file '{file_path}' loaded successfully.")
        except FileNotFoundError:
            print(f"Error: The file '{file_path}' was not found.")
            sys.exit(1)
        except Exception as e:
            print(f"An error occurred while reading the CSV file: {e}")
            sys.exit(1)

        cursor = connection.cursor()
        
        # tb_weather_aws1 테이블에 데이터를 삽입하는 SQL 쿼리입니다.
        insert_query = """
        INSERT INTO tb_weather_aws1 (
            yyyymmddhhmi, stn, wd1, ws1, wds, wss, wd10, ws10, ta, re,
            rn_15m, rn_60m, rn_12h, rn_day, hm, pa, ps, td, org_data, update_dt
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        # 여러 데이터를 한 번에 삽입합니다.
        cursor.executemany(insert_query, insert_data)

        # 변경사항을 데이터베이스에 반영합니다.
        connection.commit()
        print(f"{cursor.rowcount} record(s) inserted successfully.")

    except mariadb.Error as err:
        print(f"Error connecting to MariaDB Platform: {err}")
        # 오류가 발생하면 롤백합니다.
        if connection:
            connection.rollback()
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)
    finally:
        # 연결이 생성되었으면 닫습니다.
        if cursor:
            cursor.close()
        if connection and connection.ping():
            connection.close()
            print("MariaDB connection is closed.")
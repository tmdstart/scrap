import requests
import time
from datetime import datetime
import csv

def download_and_save_to_csv(file_url, save_path):
    try:
        response = requests.get(file_url)

        if response.status_code == 200:
            response.encoding = 'euc-kr'
            data = response.text.strip()
            lines = data.split('\n')
            
            # 데이터를 CSV 파일에 저장합니다.
            with open(save_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # 헤더와 데이터를 구분하여 저장
                header_written = False
                for line in lines:
                    line = line.strip()
                    if line.startswith('#'):
                        # #으로 시작하는 라인 중, 컬럼명을 추출하여 헤더로 사용
                        if not header_written and 'YYMMDDHHMI' in line:
                            header_line = line.replace('#', '').strip()
                            header = header_line.split()
                            writer.writerow(header)
                            header_written = True
                    elif line: # 비어있지 않은 라인만 처리 (데이터 라인)
                        row = line.split()
                        writer.writerow(row)
            
            print(f"[{datetime.now()}] 파일 다운로드 및 CSV 저장 성공: {save_path}")
        else:
            print(f"[{datetime.now()}] 파일 다운로드 실패. 상태 코드: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] 요청 중 오류 발생: {e}")
    except Exception as e:
        print(f"[{datetime.now()}] 데이터 처리 중 오류 발생: {e}")

# URL 및 인증키 설정
url_base = 'https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min'
auth_key = 'EDaMfvPZT9a2jH7z2X_WVw'  # 본인의 API 키로 변경하세요

while True:
    current_time = datetime.now().strftime('%Y%m%d%H%M')
    dynamic_url = f"{url_base}?tm2={current_time}&stn=0&disp=0&help=1&authKey={auth_key}"
    save_file_path = f"c:/data/weather/aws_data_{current_time}.csv"
    
    download_and_save_to_csv(dynamic_url, save_file_path)
    
    print("60초 후 다음 다운로드를 시작합니다...\n")
    time.sleep(60)

#import requests  # requests 모듈 임포트

#def download_file(file_url, save_path):
#    with open(save_path, 'wb') as f: # 저장할 파일을 바이너리 쓰기 모드로 열기
#        response = requests.get(file_url) # 파일 URL에 GET 요청 보내기
#        f.write(response.content) # 응답의 내용을 파일에 쓰기

# URL과 저장 경로 변수를 지정합니다.
#url = 'https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min?tm2=202302010900&stn=0&disp=0&help=1&authKey=EDaMfvPZT9a2jH7z2X_WVw'
#save_file_path = 'c:/data/weather/output_file'

# 파일 다운로드 함수를 호출합니다.
#download_file(url, save_file_path)
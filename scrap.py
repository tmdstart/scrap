import requests
from bs4 import BeautifulSoup
from io import BytesIO
from PIL import Image
import re
import os
import sqlite3
import time # 페이지 간 지연 시간을 위해 time 모듈 추가
import pymysql


# --- 전역 설정 변수 ---
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_NAME = 'my_database'
IMAGE_DIR = r'C:\gitmain\scrap\images2' # Raw 문자열로 경로 지정
# URL 변경: 이제 페이지 번호는 loop에서 추가됩니다.
NEWS_BASE_URL = "https://finance.naver.com/news/mainnews.naver?date=2024-10-25&page="
NUM_PAGES_TO_SCRAPE = 5 # 수집할 페이지 수
FILENAME_PATTERN = r'[\\/:"*?<>|]' # 파일명에 사용할 수 없는 문자 패턴
PAGE_SCRAPE_DELAY = 1 # 각 페이지 스크래핑 후 지연 시간 (초)

# --- 데이터베이스 관련 함수 ---

def connect_db():
    """MySQL 데이터베이스에 연결하고 연결 객체와 커서 객체를 반환합니다."""
    db_con = pymysql.connect(host=DB_HOST,
                             user=DB_USER,
                             password=DB_PASSWORD,
                             db=DB_NAME,)
    db_cursor = db_con.cursor()
    print("MySQL 데이터베이스에 성공적으로 연결되었습니다.")
    return db_con, db_cursor

def save_news_to_db(news_data, db_cursor, db_con):
    """수집된 뉴스 정보를 데이터베이스에 삽입합니다."""
    news_title, news_content, news_datetime, news_image_local_path, news_company = news_data
    insert_news_query = '''
    INSERT INTO NEWS (TITLE, CONTENTS, NEWSDAY, IMAGE, COMPANY)
    VALUES (%s, %s, %s, %s, %s)
    '''
    db_cursor.execute(insert_news_query, (news_title, news_content, news_datetime, news_image_local_path, news_company))
    db_con.commit()
    print(f"'{news_title}' 뉴스 정보 데이터베이스 저장 성공.")

# --- 파일 시스템 관련 함수 ---

def setup_image_directory(directory_path):
    """이미지 저장 디렉토리를 생성합니다."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"이미지 저장 디렉토리 생성: {directory_path}")

# --- 스크래핑 관련 함수 ---

def scrape_news_page(url):
    """지정된 URL에서 뉴스 페이지를 스크래핑하고 BeautifulSoup 객체를 반환합니다."""
    news_response = requests.get(url) # headers 제거
    news_response.raise_for_status() # HTTP 오류 발생 시 예외 발생
    print(f"뉴스 페이지에 성공적으로 연결되었습니다: {url}")
    return BeautifulSoup(news_response.content, 'html.parser')

def download_and_save_image(full_image_url, news_title, index, image_dir, pattern):
    """이미지를 다운로드하고 로컬에 저장한 후 저장 경로를 반환합니다."""
    news_image_local_path = ""
    image_response = requests.get(full_image_url) # headers 제거
    image_response.raise_for_status() # HTTP 오류 발생 시 예외 발생

    content_type = image_response.headers.get('Content-Type', '')
    if 'image' not in content_type:
        print(f"경고: '{news_title}'의 URL에서 이미지 대신 다른 콘텐츠({content_type})를 받았습니다. 로컬 저장 스킵.")
    else:
        img = Image.open(BytesIO(image_response.content))
        image_filename = re.sub(pattern, '', news_title)
        if not image_filename:
            image_filename = f"untitled_news_{index}"
        save_path = os.path.join(image_dir, image_filename + ".png")
        img.save(save_path)
        print(f"이미지 로컬 저장 성공: {save_path}")
        news_image_local_path = save_path
    return news_image_local_path

def parse_news_item(item_soup, index):
    """하나의 뉴스 아이템(BeautifulSoup 객체)에서 정보를 파싱합니다."""
    title_tag = item_soup.select_one('dl > dd.articleSubject > a')
    image_tag = item_soup.select_one('dl > dt > a > img')
    summary_tag = item_soup.select_one('dl > dd.articleSummary')
    date_tag = item_soup.select_one('dl > dd.articleSummary > span.wdate')
    company_tag = item_soup.select_one('dl > dd.articleSummary > span.press')

    news_title = ""
    news_content = ""
    news_datetime = ""
    news_image_local_path = ""
    news_company = ""

    if title_tag:
        news_title = title_tag.text.strip()
    else:
        print(f"경고: {index+1}번째 뉴스 아이템에서 제목을 찾을 수 없습니다.")
        return None # 제목이 없으면 이 아이템은 건너김

    if image_tag and image_tag.get('src'):
        image_src = image_tag.get('src')
        if image_src.startswith('//'):
            full_image_url = "https:" + image_src
        else:
            full_image_url = image_src

        # 이미지 다운로드 및 로컬 저장
        news_image_local_path = download_and_save_image(full_image_url, news_title, index, IMAGE_DIR, FILENAME_PATTERN)
    else:
        print(f"경고: {index+1}번째 뉴스 '{news_title}'에서 이미지를 찾을 수 없습니다.")

    if date_tag:
        news_datetime = date_tag.text.strip()
    else:
        print(f"경고: {index+1}번째 뉴스 '{news_title}'에서 날짜를 찾을 수 없습니다.")

    if company_tag:
        news_company = company_tag.text.strip()
    else:
        print(f"경고: {index+1}번째 뉴스 '{news_title}'에서 회사를 찾을 수 없습니다.")

    if summary_tag:
        temp_text = summary_tag.text.strip()
        if date_tag:
            temp_text = temp_text.replace(date_tag.text.strip(), '').strip()
        if company_tag:
            temp_text = temp_text.replace(company_tag.text.strip(), '').strip()
        news_content = temp_text
    else:
        print(f"경고: {index+1}번째 뉴스 '{news_title}'에서 요약/내용을 찾을 수 없습니다.")

    return (news_title, news_content, news_datetime, news_image_local_path, news_company)





"""뉴스 스크래핑 및 데이터베이스 저장의 전체 과정을 실행합니다."""
db_con, db_cursor = connect_db()
setup_image_directory(IMAGE_DIR)

total_scraped_news = 0
for page_num in range(1, NUM_PAGES_TO_SCRAPE + 1):
    current_page_url = f"{NEWS_BASE_URL}{page_num}"
    print(f"\n--- {page_num} 페이지 스크래핑 시작 ---")
    soup = scrape_news_page(current_page_url)
    news_items = soup.select("#contentarea_left > div.mainNewsList._replaceNewsLink > ul > li")

    print(f"현재 페이지({page_num})에서 수집할 뉴스 기사 수: {len(news_items)}")
    if not news_items:
        print(f"경고: {page_num} 페이지에서 뉴스 아이템을 찾을 수 없습니다. 셀렉터가 변경되었거나 마지막 페이지일 수 있습니다.")
        # 더 이상 뉴스가 없으면 다음 페이지로 진행하지 않음
        break

    for i, item in enumerate(news_items):
        news_data = parse_news_item(item, i)
        if news_data: # 뉴스 데이터가 성공적으로 파싱된 경우에만 저장
            print(f"수집 정보: 제목='{news_data[0]}', 날짜='{news_data[2]}', 회사='{news_data[4]}', 이미지경로='{news_data[3]}', 내용='{news_data[1][:50]}...'")
            save_news_to_db(news_data, db_cursor, db_con)
            total_scraped_news += 1

    # 다음 페이지로 넘어가기 전에 지연 시간 추가 (봇 감지 회피)
    if page_num < NUM_PAGES_TO_SCRAPE:
        print(f"{PAGE_SCRAPE_DELAY}초 대기 후 다음 페이지로 이동...")
        time.sleep(PAGE_SCRAPE_DELAY)

db_con.close()
print(f"\n데이터베이스 연결이 닫혔습니다. 총 {total_scraped_news}개의 뉴스 기사를 수집했습니다.")
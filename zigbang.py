from playwright.sync_api import sync_playwright
import time
import re

def parse_room_info(text, info_type):
    """추출된 텍스트에서 방 정보를 파싱하는 함수"""
    parsed = {}
    
    # 가격 정보 파싱
    if "가격정보" in info_type:
        # 보증금/월세 패턴
        deposit_rent = re.search(r'보증금\s*(\d+(?:,\d+)*)\s*만원?\s*월세\s*(\d+(?:,\d+)*)\s*만원?', text, re.IGNORECASE)
        if deposit_rent:
            parsed['보증금'] = deposit_rent.group(1) + '만원'
            parsed['월세'] = deposit_rent.group(2) + '만원'
        
        # 전세 패턴
        jeonse = re.search(r'전세\s*(\d+(?:,\d+)*)\s*만원?', text, re.IGNORECASE)
        if jeonse:
            parsed['전세'] = jeonse.group(1) + '만원'
        
        # 매매 패턴
        sale = re.search(r'매매\s*(\d+(?:,\d+)*)\s*만원?', text, re.IGNORECASE)
        if sale:
            parsed['매매'] = sale.group(1) + '만원'
        
        # 간단한 가격 패턴 (예: 1,000/50)
        simple_price = re.search(r'(\d+(?:,\d+)*)\s*/\s*(\d+(?:,\d+)*)', text)
        if simple_price and not deposit_rent:
            parsed['보증금'] = simple_price.group(1) + '만원'
            parsed['월세'] = simple_price.group(2) + '만원'
    
    # 상세 방정보 파싱
    elif "상세방정보" in info_type:
        # 면적 정보
        area = re.search(r'(\d+(?:\.\d+)?)\s*평', text)
        if area:
            parsed['평수'] = area.group(1) + '평'
        
        area_m2 = re.search(r'(\d+(?:\.\d+)?)\s*㎡', text)
        if area_m2:
            parsed['면적'] = area_m2.group(1) + '㎡'
        
        # 층수 정보
        floor = re.search(r'(\d+)\s*층', text)
        if floor:
            parsed['층수'] = floor.group(1) + '층'
        
        # 관리비
        maintenance = re.search(r'관리비\s*(\d+(?:,\d+)*)\s*만원?', text, re.IGNORECASE)
        if maintenance:
            parsed['관리비'] = maintenance.group(1) + '만원'
        
        # 방 타입
        room_types = ['원룸', '투룸', '쓰리룸', '오피스텔', '아파트', '빌라']
        for room_type in room_types:
            if room_type in text:
                parsed['방타입'] = room_type
                break
        
        # 입주 가능일
        move_in = re.search(r'입주\s*가능일?\s*[:\-]?\s*([^\n\r]+)', text, re.IGNORECASE)
        if move_in:
            parsed['입주가능일'] = move_in.group(1).strip()
        
        # 주차 정보
        if '주차' in text:
            parking = re.search(r'주차\s*[:\-]?\s*([^\n\r]+)', text, re.IGNORECASE)
            if parking:
                parsed['주차'] = parking.group(1).strip()
        
        # 엘리베이터
        if '엘리베이터' in text:
            parsed['엘리베이터'] = '있음'
        
        # 건물 연도
        year = re.search(r'(\d{4})\s*년', text)
        if year:
            parsed['건축연도'] = year.group(1) + '년'
    
    return parsed

def scrape_all_room_info(page):
    """페이지에서 모든 방 정보 추출"""
    
    # 스크래핑할 특정 CSS 셀렉터들
    target_selectors = {
        "가격정보1": "#__next > div.sc-3fe88b04-0.cxrepQ > div > div.sc-f98dc6d6-2.gxoAju > div > div > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-14lw9ot.r-13awgt0 > div > div > div:nth-child(1)",
        "가격정보2": "#__next > div.sc-3fe88b04-0.cxrepQ > div > div.sc-f98dc6d6-2.gxoAju > div > div > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-14lw9ot.r-13awgt0 > div > div > div:nth-child(2)",
        "가격정보3": "#__next > div.sc-3fe88b04-0.cxrepQ > div > div.sc-f98dc6d6-2.gxoAju > div > div > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-14lw9ot.r-13awgt0 > div > div > div:nth-child(3)",
        "상세방정보": "#__next > div.sc-3fe88b04-0.cxrepQ > div > div.sc-f98dc6d6-2.gxoAju > div > div > div:nth-child(2) > div:nth-child(2) > div > div.css-1dbjc4n.r-150rngu.r-14lw9ot.r-eqz5dr.r-16y2uox.r-1wbh5a2.r-11yh6sk.r-1rnoaur.r-1sncvnh > div"
    }
    
    extracted_data = []
    
    for name, selector in target_selectors.items():
        try:
            print(f"\n📍 {name} 추출 중...")
            
            # 요소 찾기 (상세정보가 펼쳐진 후이므로 짧은 대기시간)
            elements = page.query_selector_all(selector)
            
            if elements:
                for i, element in enumerate(elements):
                    # 순수 텍스트 내용만 추출
                    clean_text = page.evaluate("""
                        (elem) => {
                            if (!elem) return '';
                            
                            // 모든 텍스트 노드를 추출하되, 스타일과 스크립트는 제외
                            const walker = document.createTreeWalker(
                                elem,
                                NodeFilter.SHOW_TEXT,
                                {
                                    acceptNode: function(node) {
                                        const parent = node.parentElement;
                                        if (parent && (parent.tagName === 'SCRIPT' || parent.tagName === 'STYLE')) {
                                            return NodeFilter.FILTER_REJECT;
                                        }
                                        if (!node.textContent.trim()) {
                                            return NodeFilter.FILTER_REJECT;
                                        }
                                        return NodeFilter.FILTER_ACCEPT;
                                    }
                                }
                            );
                            
                            const textNodes = [];
                            let node;
                            while (node = walker.nextNode()) {
                                const text = node.textContent.trim();
                                if (text && text.length > 0) {
                                    textNodes.push(text);
                                }
                            }
                            
                            // 중복 제거하고 정리
                            const uniqueTexts = [...new Set(textNodes)]
                                .join(' ')
                                .replace(/\s+/g, ' ')
                                .trim();
                            
                            return uniqueTexts;
                        }
                    """, element)
                    
                    if clean_text:
                        # 방 정보 파싱
                        parsed_info = parse_room_info(clean_text, name)
                        
                        element_name = f"{name}" if len(elements) == 1 else f"{name}-{i+1}"
                        
                        extracted_data.append({
                            'name': element_name,
                            'selector': selector,
                            'raw_text': clean_text,
                            'parsed_info': parsed_info,
                            'success': True
                        })
                        
                        print(f"✅ {element_name} 추출 성공!")
                        print(f"   텍스트 길이: {len(clean_text)} 글자")
                        print(f"   텍스트 내용: {clean_text[:100]}{'...' if len(clean_text) > 100 else ''}")
                    
            else:
                extracted_data.append({
                    'name': name,
                    'selector': selector,
                    'error': '요소를 찾을 수 없음',
                    'success': False
                })
                print(f"❌ {name} 추출 실패: 요소를 찾을 수 없음")
                
        except Exception as e:
            extracted_data.append({
                'name': name,
                'selector': selector,
                'error': str(e),
                'success': False
            })
            print(f"❌ {name} 추출 실패: {e}")
    
    return extracted_data

def extract_room_details_after_click():
    """버튼 클릭 후 상세정보 추출 전용 함수"""
    
    url = "https://www.zigbang.com/home/oneroom/items/45759886?itemDetailType=ZIGBANG&imageThumbnail=https%3A%2F%2Fic.zigbang.com%2Fic%2Fitems%2F45759886%2F1.jpg&hasVrKey=false"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        try:
            print("🔄 페이지 접속 중...")
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            time.sleep(5)
            
            # 상세정보 버튼 클릭
            detail_button_selector = "#__next > div.sc-3fe88b04-0.cxrepQ > div > div.sc-f98dc6d6-2.gxoAju > div > div > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-14lw9ot.r-13awgt0 > div > div > div:nth-child(3) > div.css-1dbjc4n.r-18u37iz.r-1ah4tor > div.css-1dbjc4n.r-1awozwy.r-18u37iz.r-17s6mgv.r-5oul0u.r-1joea0r.r-knv0ih > div > div > div.css-1563yu1.r-ajo9tl.r-1wbh5a2.r-1w6e6rj.r-159m18f.r-1b43r93.r-majxgm.r-rjixqe.r-1d4mawv.r-1ff274t.r-13wfysu.r-q42fyq.r-1ad0z5i"
            
            print("\n🔘 상세정보 버튼 찾는 중...")
            
            # 버튼 클릭을 여러 방법으로 시도
            button_clicked = False
            
            # 방법 1: 정확한 셀렉터로 클릭
            try:
                detail_button = page.wait_for_selector(detail_button_selector, timeout=10000)
                if detail_button:
                    button_text = detail_button.inner_text()
                    print(f"✅ 버튼 발견! 텍스트: '{button_text}'")
                    detail_button.click()
                    button_clicked = True
                    print("👆 정확한 셀렉터로 버튼 클릭 성공!")
            except Exception as e:
                print(f"⚠️ 정확한 셀렉터 클릭 실패: {e}")
            
            # 방법 2: 텍스트로 버튼 찾기 (백업)
            if not button_clicked:
                try:
                    print("🔍 텍스트로 버튼 찾는 중...")
                    text_buttons = ["더보기", "상세정보", "펼치기", "더 보기", "자세히"]
                    
                    for button_text in text_buttons:
                        buttons = page.get_by_text(button_text)
                        if buttons.count() > 0:
                            print(f"✅ '{button_text}' 버튼 발견!")
                            buttons.first().click()
                            button_clicked = True
                            print(f"👆 '{button_text}' 버튼 클릭 성공!")
                            break
                            
                except Exception as e:
                    print(f"⚠️ 텍스트 버튼 클릭 실패: {e}")
            
            # 방법 3: 클릭 가능한 div 요소 찾기 (최후 수단)
            if not button_clicked:
                try:
                    print("🔍 클릭 가능한 요소 찾는 중...")
                    clickable_divs = page.query_selector_all("div[class*='r-1ad0z5i']")
                    
                    for div in clickable_divs:
                        text = div.inner_text().strip()
                        if any(keyword in text.lower() for keyword in ['더', '보기', '상세', '펼치기']):
                            print(f"✅ 클릭 가능한 요소 발견: '{text}'")
                            div.click()
                            button_clicked = True
                            print("👆 클릭 가능한 요소 클릭 성공!")
                            break
                            
                except Exception as e:
                    print(f"⚠️ 클릭 가능한 요소 찾기 실패: {e}")
            
            if button_clicked:
                print("⏳ 상세정보 펼쳐지는 중... (5초 대기)")
                time.sleep(5)
                page.screenshot(path='after_detail_click.png')
                print("📸 상세정보 펼친 후 스크린샷: after_detail_click.png")
            else:
                print("❌ 상세정보 버튼을 클릭할 수 없었습니다. 기본 정보만 추출합니다.")
            
            # 이제 모든 정보 추출
            extracted_data = scrape_all_room_info(page)
            print_extraction_results(extracted_data)
            
            return extracted_data
            
        except Exception as e:
            print(f"💥 전체 오류 발생: {e}")
            # 오류 발생시 스크린샷 저장
            page.screenshot(path='error_screenshot.png')
            print("📸 오류 스크린샷이 error_screenshot.png로 저장되었습니다.")
            return None
            
        finally:
            browser.close()

def scrape_all_room_info(page):
    """페이지에서 모든 방 정보 추출"""
    
    # 스크래핑할 특정 CSS 셀렉터들
    target_selectors = {
        "가격정보1": "#__next > div.sc-3fe88b04-0.cxrepQ > div > div.sc-f98dc6d6-2.gxoAju > div > div > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-14lw9ot.r-13awgt0 > div > div > div:nth-child(1)",
        "가격정보2": "#__next > div.sc-3fe88b04-0.cxrepQ > div > div.sc-f98dc6d6-2.gxoAju > div > div > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-14lw9ot.r-13awgt0 > div > div > div:nth-child(2)",
        "가격정보3": "#__next > div.sc-3fe88b04-0.cxrepQ > div > div.sc-f98dc6d6-2.gxoAju > div > div > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-13awgt0 > div.css-1dbjc4n.r-14lw9ot.r-13awgt0 > div > div > div:nth-child(3)",
        "상세방정보": "#__next > div.sc-3fe88b04-0.cxrepQ > div > div.sc-f98dc6d6-2.gxoAju > div > div > div:nth-child(2) > div:nth-child(2) > div > div.css-1dbjc4n.r-150rngu.r-14lw9ot.r-eqz5dr.r-16y2uox.r-1wbh5a2.r-11yh6sk.r-1rnoaur.r-1sncvnh > div"
    }
    
    extracted_data = []
    
    for name, selector in target_selectors.items():
        try:
            print(f"\n📍 {name} 추출 중...")
            
            # 요소 찾기
            elements = page.query_selector_all(selector)
            
            if elements:
                for i, element in enumerate(elements):
                    # 순수 텍스트 내용만 추출
                    clean_text = page.evaluate("""
                        (elem) => {
                            if (!elem) return '';
                            
                            // 모든 텍스트 노드를 추출하되, 스타일과 스크립트는 제외
                            const walker = document.createTreeWalker(
                                elem,
                                NodeFilter.SHOW_TEXT,
                                {
                                    acceptNode: function(node) {
                                        const parent = node.parentElement;
                                        if (parent && (parent.tagName === 'SCRIPT' || parent.tagName === 'STYLE')) {
                                            return NodeFilter.FILTER_REJECT;
                                        }
                                        if (!node.textContent.trim()) {
                                            return NodeFilter.FILTER_REJECT;
                                        }
                                        return NodeFilter.FILTER_ACCEPT;
                                    }
                                }
                            );
                            
                            const textNodes = [];
                            let node;
                            while (node = walker.nextNode()) {
                                const text = node.textContent.trim();
                                if (text && text.length > 0) {
                                    textNodes.push(text);
                                }
                            }
                            
                            // 중복 제거하고 정리
                            const uniqueTexts = [...new Set(textNodes)]
                                .join(' ')
                                .replace(/\s+/g, ' ')
                                .trim();
                            
                            return uniqueTexts;
                        }
                    """, element)
                    
                    if clean_text:
                        # 방 정보 파싱
                        parsed_info = parse_room_info(clean_text, name)
                        
                        element_name = f"{name}" if len(elements) == 1 else f"{name}-{i+1}"
                        
                        extracted_data.append({
                            'name': element_name,
                            'selector': selector,
                            'raw_text': clean_text,
                            'parsed_info': parsed_info,
                            'success': True
                        })
                        
                        print(f"✅ {element_name} 추출 성공!")
                        print(f"   텍스트 길이: {len(clean_text)} 글자")
                        print(f"   텍스트 내용: {clean_text[:100]}{'...' if len(clean_text) > 100 else ''}")
                    
            else:
                extracted_data.append({
                    'name': name,
                    'selector': selector,
                    'error': '요소를 찾을 수 없음',
                    'success': False
                })
                print(f"❌ {name} 추출 실패: 요소를 찾을 수 없음")
                
        except Exception as e:
            extracted_data.append({
                'name': name,
                'selector': selector,
                'error': str(e),
                'success': False
            })
            print(f"❌ {name} 추출 실패: {e}")
    
    return extracted_data

def print_extraction_results(extracted_data):
    """추출 결과를 깔끔하게 출력하는 함수 (텍스트만 포커스)"""
    print("\n" + "="*100)
    print("🏠 직방 방 정보 추출 결과")
    print("="*100)
    
    successful_extractions = 0
    
    for data in extracted_data:
        print(f"\n📍 {data['name']}:")
        print("-" * 80)
        
        if data['success']:
            successful_extractions += 1
            print(f"✅ 상태: 추출 성공")
            
            # 원본 텍스트
            print(f"📝 원본 텍스트:")
            print(f"   {data['raw_text']}")
            
            # 파싱된 정보
            if data['parsed_info']:
                print(f"🔍 파싱된 정보:")
                for key, value in data['parsed_info'].items():
                    print(f"   • {key}: {value}")
            else:
                print(f"🔍 파싱된 정보: 해당없음")
            
        else:
            print(f"❌ 상태: 추출 실패")
            print(f"📝 오류: {data.get('error', '알 수 없는 오류')}")
    
    print("\n" + "="*100)
    print(f"📊 추출 요약: {successful_extractions}/{len(extracted_data)} 개 요소 성공")
    print("="*100)

def scrape_zigbang_specific_elements():
    """직방 특정 CSS 셀렉터 요소들 스크래핑 (백업 함수)"""
    
    url = "https://www.zigbang.com/home/oneroom/items/45759886?itemDetailType=ZIGBANG&imageThumbnail=https%3A%2F%2Fic.zigbang.com%2Fic%2Fitems%2F45759886%2F1.jpg&hasVrKey=false"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        try:
            print("🔄 백업 스크래핑 - 페이지 접속 중...")
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            time.sleep(5)
            
            extracted_data = scrape_all_room_info(page)
            print_extraction_results(extracted_data)
            
            return extracted_data
            
        except Exception as e:
            print(f"💥 백업 스크래핑 오류: {e}")
            return None
            
        finally:
            browser.close()

# 더 유연한 셀렉터 버전 (클래스명이 동적으로 변경될 경우 대비)
def scrape_with_flexible_selectors():
    """더 유연한 셀렉터를 사용한 스크래핑"""
    
    url = "https://www.zigbang.com/home/oneroom/items/45759886?itemDetailType=ZIGBANG&imageThumbnail=https%3A%2F%2Fic.zigbang.com%2Fic%2Fitems%2F45759886%2F1.jpg&hasVrKey=false"
    
    # 더 유연한 셀렉터들 (클래스명 변경에 대비)
    flexible_selectors = {
        "가격정보_유연1": "div[class*='r-14lw9ot'][class*='r-13awgt0'] > div > div > div:nth-child(1)",
        "가격정보_유연2": "div[class*='r-14lw9ot'][class*='r-13awgt0'] > div > div > div:nth-child(2)", 
        "가격정보_유연3": "div[class*='r-14lw9ot'][class*='r-13awgt0'] > div > div > div:nth-child(3)",
        "상세정보_유연": "div[class*='r-150rngu'][class*='r-14lw9ot'] > div"
    }
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        try:
            print("🔄 유연한 셀렉터로 페이지 접속 중...")
            page.goto(url, timeout=30000)
            time.sleep(3)
            
            print("🎯 유연한 셀렉터로 방 정보 추출 중...")
            
            for name, selector in flexible_selectors.items():
                try:
                    elements = page.query_selector_all(selector)
                    if elements:
                        for j, elem in enumerate(elements):
                            text = elem.inner_text().strip()
                            if text:  # 빈 텍스트가 아닌 경우만
                                parsed = parse_room_info(text, name)
                                print(f"📍 {name}-{j+1}:")
                                print(f"   텍스트: {text[:100]}...")
                                if parsed:
                                    print(f"   파싱: {parsed}")
                    else:
                        print(f"❌ {name}: 요소 없음")
                except Exception as e:
                    print(f"❌ {name} 오류: {e}")
                    
        finally:
            browser.close()

# 메인 실행 함수
def main():
    print("🚀 직방 방 정보 스크래핑 시작!")
    print("🔘 상세정보 버튼 클릭 후 데이터 추출 진행...")
    
    # 상세정보 버튼 클릭 후 추출
    result = extract_room_details_after_click()
    
    # 실패한 경우 기본 스크래핑 시도
    if not result or not any(data['success'] for data in result):
        print("\n🔄 기본 스크래핑으로 재시도...")
        scrape_zigbang_specific_elements()
    
    print("\n✅ 스크래핑 완료!")

if __name__ == "__main__":
    main()
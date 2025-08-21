from playwright.sync_api import sync_playwright
import time

def scrape_zigbang_room_info():
    """직방 특정 매물의 정보 스크래핑 """
    
    # 스크래핑할 URL
    url = "https://www.zigbang.com/home/oneroom/items/45759886?itemDetailType=ZIGBANG&imageThumbnail=https%3A%2F%2Fic.zigbang.com%2Fic%2Fitems%2F45759886%2F1.jpg&hasVrKey=false"
    
    with sync_playwright() as p:
        # 브라우저 시작 (User-Agent 추가)
        browser = p.chromium.launch(
            headless=False,
            args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        try:
            print(" 페이지 접속 중...")
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            # 더 긴 대기 시간
            print(" 페이지 로딩 대기 중...")
            time.sleep(5)
            
            # 페이지가 제대로 로드됐는지 확인
            page_title = page.title()
            print(f" 페이지 제목: {page_title}")
            
            # 스크린샷으로 페이지 상태 확인
            page.screenshot(path='page_loaded.png')
            print(" 페이지 스크린샷 저장: page_loaded.png")
            
            print(" 방 정보 추출 중...")
            
            # 먼저 페이지의 모든 텍스트를 확인
            all_text = page.evaluate("() => document.body.innerText")
            print(f" 페이지 텍스트 길이: {len(all_text)} 글자")
            print(f" 페이지 텍스트 일부:\n{all_text[:500]}...")
            
            # 모든 셀렉터를 테스트해보기
            test_selectors = [
                'h1', 'h2', 'h3',
                '.price', '.price-info', '.item-price',
                '.location', '.address',
                '.area', '.size',
                'div[class*="price"]',
                'div[class*="info"]',
                'span[class*="price"]',
                'p[class*="price"]'
            ]
            
            print("\n 셀렉터 테스트 결과:")
            for selector in test_selectors:
                elements = page.query_selector_all(selector)
                if elements:
                    print(f" {selector}: {len(elements)}개 요소 발견")
                    for i, elem in enumerate(elements[:3]):  # 처음 3개만
                        try:
                            text = elem.inner_text().strip()[:100]
                            if text:
                                print(f"   [{i+1}] {text}")
                        except:
                            pass
                else:
                    print(f" {selector}: 요소 없음")
            
            # 개선된 정보 추출
            room_info = page.evaluate("""
                () => {
                    const result = {};
                    
                    // 모든 텍스트를 가져와서 패턴으로 찾기
                    const allText = document.body.innerText;
                    result.allText = allText.slice(0, 1000);
                    
                    // 가격 패턴 찾기
                    const pricePatterns = [
                        /보증금\s*(\d+(?:,\d+)*)\s*만원?\s*월세\s*(\d+(?:,\d+)*)\s*만원?/gi,
                        /(\d+(?:,\d+)*)\s*\/\s*(\d+(?:,\d+)*)/g,
                        /보증금\s*(\d+(?:,\d+)*)/gi,
                        /월세\s*(\d+(?:,\d+)*)/gi
                    ];
                    
                    result.priceMatches = [];
                    pricePatterns.forEach((pattern, i) => {
                        const matches = [...allText.matchAll(pattern)];
                        if (matches.length > 0) {
                            result.priceMatches.push(`패턴${i+1}: ${matches.map(m => m[0]).join(', ')}`);
                        }
                    });
                    
                    // 일반적인 정보들 찾기
                    const keywords = ['평', '층', '관리비', '입주', '주차', '엘리베이터'];
                    result.keywordMatches = {};
                    
                    keywords.forEach(keyword => {
                        const regex = new RegExp(`[^\\n]*${keyword}[^\\n]*`, 'gi');
                        const matches = allText.match(regex);
                        if (matches) {
                            result.keywordMatches[keyword] = matches.slice(0, 3);
                        }
                    });
                    
                    // DOM 요소에서 직접 찾기
                    const h1 = document.querySelector('h1');
                    result.title = h1 ? h1.innerText.trim() : '제목 없음';
                    
                    // 가격이 포함된 요소 찾기
                    const allDivs = document.querySelectorAll('div, span, p');
                    result.priceElements = [];
                    
                    allDivs.forEach(elem => {
                        const text = elem.innerText || elem.textContent || '';
                        if (text.includes('만원') || text.includes('보증금') || text.includes('월세') || /\d+\/\d+/.test(text)) {
                            if (text.trim().length < 200) {  // 너무 긴 텍스트 제외
                                result.priceElements.push(text.trim());
                            }
                        }
                    });
                    
                    // 중복 제거
                    result.priceElements = [...new Set(result.priceElements)].slice(0, 10);
                    
                    return result;
                }
            """)
            
            # 결과 출력
            print_detailed_info(room_info)
            
        except Exception as e:
            print(f" 오류 발생: {e}")
            # 오류 발생시 스크린샷 저장
            page.screenshot(path='error_screenshot.png')
            print(" 오류 스크린샷이 error_screenshot.png로 저장되었습니다.")
            
        finally:
            browser.close()

def print_detailed_info(room_info):
    """매물 정보를 자세히 출력하는 함수"""
    print("\n" + "="*80)
    print(" 직방 매물 정보 분석 결과")
    print("="*80)
    
    print(f" 추출된 제목: {room_info.get('title', '없음')}")
    
    print("\n 가격 패턴 매칭 결과:")
    if room_info.get('priceMatches'):
        for match in room_info['priceMatches']:
            print(f"   {match}")
    else:
        print("   가격 패턴을 찾을 수 없습니다.")
    
    print("\n 가격 관련 요소들:")
    if room_info.get('priceElements'):
        for i, elem in enumerate(room_info['priceElements'], 1):
            print(f"   [{i}] {elem}")
    else:
        print("   가격 관련 요소를 찾을 수 없습니다.")
    
    print("\n🔑 키워드 매칭 결과:")
    if room_info.get('keywordMatches'):
        for keyword, matches in room_info['keywordMatches'].items():
            print(f"   {keyword}: {matches}")
    else:
        print("   키워드 매칭 결과가 없습니다.")
    
    print("\n 페이지 텍스트 일부:")
    print("-" * 60)
    print(room_info.get('allText', '텍스트 없음'))
    print("="*80)

# 특정 URL 테스트 함수
def test_url_access(test_url=None):
    """URL 접근 테스트"""
    if not test_url:
        test_url = "https://www.zigbang.com"  # 메인 페이지 먼저 테스트
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        try:
            print(f" URL 접근 테스트: {test_url}")
            page.goto(test_url, timeout=30000)
            
            title = page.title()
            print(f" 접근 성공! 제목: {title}")
            
            # 간단한 텍스트 추출
            text = page.evaluate("() => document.body.innerText.slice(0, 200)")
            print(f" 페이지 텍스트: {text}...")
            
            page.screenshot(path='test_screenshot.png')
            print(" 테스트 스크린샷: test_screenshot.png")
            
        except Exception as e:
            print(f" 접근 실패: {e}")
        finally:
            browser.close()

# 메인 실행 부분
def main():
    print(" 개선된 직방 매물 정보 스크래핑 시작!")
    
    # 1. 먼저 간단한 접근 테스트
    print("\n1 사이트 접근 테스트")
    test_url_access()
    
    input("\n계속하려면 Enter를 누르세요...")
    
    # 2. 실제 매물 페이지 스크래핑
    print("\n 매물 페이지 스크래핑")
    scrape_zigbang_room_info()
    
    print(" 스크래핑 완료!")

if __name__ == "__main__":
    main()
    
    
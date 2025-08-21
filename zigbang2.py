from playwright.sync_api import sync_playwright
import time
import json
import csv
from datetime import datetime

def scrape_zigbang_sidebar_rooms():
    """직방 매물 검색 페이지의 오른쪽 사이드바 매물 목록 10개 스크래핑"""
    
    # 스크래핑할 URL
    url = "https://www.zigbang.com/home/oneroom/items?lat_south=37.5264320373535&lat_north=37.5264320373535&lng_west=126.896011352539&lng_east=126.896011352539&need_more_zoom_in=false&isZikimFiltered=false&mapLatitude=37.5264320373535&mapLongitude=126.896011352539"
    
    with sync_playwright() as p:
        # 브라우저 시작
        browser = p.chromium.launch(
            headless=False,  # True로 변경하면 백그라운드 실행
            args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        try:
            print("🔗 페이지 접속 중...")
            page.goto(url, wait_until='domcontentloaded', timeout=30000)
            
            print("⏳ 페이지 로딩 대기 중...")
            time.sleep(5)  # 매물 목록이 로드될 때까지 대기
            
            # 페이지 로드 확인용 스크린샷
            page.screenshot(path='sidebar_page.png')
            print("📸 페이지 스크린샷 저장: sidebar_page.png")
            
            print("🏠 사이드바 매물 목록 추출 중...")
            
            # 사이드바의 매물 목록 추출
            rooms_data = page.evaluate("""
                () => {
                    const rooms = [];
                    
                    // 사이드바 매물 카드들을 찾는 다양한 선택자 시도
                    const selectors = [
                        '.item-card',           // 매물 카드
                        '.room-item',           // 방 아이템
                        '.property-item',       // 부동산 아이템
                        '.list-item',           // 리스트 아이템
                        '[class*="item"]',      // item이 포함된 클래스
                        '[class*="card"]',      // card가 포함된 클래스
                        '.sidebar .item',       // 사이드바 내 아이템
                        '.right-panel .item',   // 오른쪽 패널 아이템
                    ];
                    
                    let roomElements = [];
                    
                    // 각 선택자로 요소들 찾기
                    for (const selector of selectors) {
                        const elements = document.querySelectorAll(selector);
                        if (elements.length > 0) {
                            console.log(`${selector}: ${elements.length}개 발견`);
                            roomElements = Array.from(elements);
                            break;
                        }
                    }
                    
                    // 만약 위 선택자들로 찾지 못했다면, 더 일반적인 방법 사용
                    if (roomElements.length === 0) {
                        // 사이드바나 오른쪽 영역에서 매물로 보이는 요소들 찾기
                        const allDivs = document.querySelectorAll('div');
                        allDivs.forEach(div => {
                            const text = div.innerText || '';
                            // 가격 정보가 있고, 적당한 크기의 요소라면 매물 카드일 가능성
                            if (text.includes('만원') && text.length > 50 && text.length < 500) {
                                roomElements.push(div);
                            }
                        });
                    }
                    
                    console.log(`총 ${roomElements.length}개의 매물 요소 발견`);
                    
                    // 처음 10개만 처리
                    const limitedElements = roomElements.slice(0, 10);
                    
                    limitedElements.forEach((element, index) => {
                        try {
                            const roomData = {
                                index: index + 1,
                                title: '',
                                price: '',
                                area: '',
                                floor: '',
                                location: '',
                                link: '',
                                fullText: ''
                            };
                            
                            // 전체 텍스트 가져오기
                            const fullText = element.innerText || element.textContent || '';
                            roomData.fullText = fullText.trim();
                            
                            // 링크 찾기
                            const linkElement = element.querySelector('a') || element.closest('a');
                            if (linkElement) {
                                const href = linkElement.getAttribute('href');
                                if (href) {
                                    roomData.link = href.startsWith('http') ? href : 'https://www.zigbang.com' + href;
                                }
                            }
                            
                            // 제목 추출 (여러 방법 시도)
                            const titleSelectors = ['h3', 'h4', '.title', '.name', '[class*="title"]'];
                            for (const selector of titleSelectors) {
                                const titleElement = element.querySelector(selector);
                                if (titleElement && titleElement.innerText.trim()) {
                                    roomData.title = titleElement.innerText.trim();
                                    break;
                                }
                            }
                            
                            // 제목을 찾지 못했다면 첫 번째 줄 사용
                            if (!roomData.title) {
                                const lines = fullText.split('\\n').filter(line => line.trim());
                                if (lines.length > 0) {
                                    roomData.title = lines[0].substring(0, 50); // 첫 50자만
                                }
                            }
                            
                            // 가격 정보 추출
                            const priceMatch = fullText.match(/(보증금|전세|매매)\\s*([\\d,]+)\\s*만원?|([\\d,]+)\\s*\\/\\s*([\\d,]+)|([\\d,]+)\\s*만원/gi);
                            if (priceMatch) {
                                roomData.price = priceMatch[0];
                            }
                            
                            // 면적 정보 추출
                            const areaMatch = fullText.match(/([\\d.]+)\\s*평|([\\d.]+)\\s*㎡/);
                            if (areaMatch) {
                                roomData.area = areaMatch[0];
                            }
                            
                            // 층수 정보 추출
                            const floorMatch = fullText.match(/([\\d]+)\\s*층/);
                            if (floorMatch) {
                                roomData.floor = floorMatch[0];
                            }
                            
                            // 위치 정보 추출 (주소나 동네 이름)
                            const locationMatch = fullText.match(/[가-힣]+구\\s+[가-힣]+동|[가-힣]+시\\s+[가-힣]+구/);
                            if (locationMatch) {
                                roomData.location = locationMatch[0];
                            }
                            
                            rooms.push(roomData);
                            
                        } catch (error) {
                            console.error(`매물 ${index + 1} 처리 중 오류:`, error);
                        }
                    });
                    
                    return {
                        rooms: rooms,
                        totalFound: roomElements.length,
                        extracted: rooms.length
                    };
                }
            """)
            
            # 결과 출력
            print_rooms_info(rooms_data)
            
            # 결과 저장
            save_results(rooms_data['rooms'])
            
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            page.screenshot(path='error_sidebar.png')
            print("📸 오류 스크린샷 저장: error_sidebar.png")
            
        finally:
            browser.close()

def print_rooms_info(rooms_data):
    """매물 정보를 출력하는 함수"""
    print("\n" + "="*80)
    print("🏠 직방 사이드바 매물 목록")
    print("="*80)
    
    print(f"📊 발견된 총 요소 수: {rooms_data['totalFound']}개")
    print(f"📋 추출된 매물 수: {rooms_data['extracted']}개")
    print("-"*80)
    
    for room in rooms_data['rooms']:
        print(f"\n🏠 매물 {room['index']}")
        print(f"   제목: {room['title'] or '정보 없음'}")
        print(f"   가격: {room['price'] or '정보 없음'}")
        print(f"   면적: {room['area'] or '정보 없음'}")
        print(f"   층수: {room['floor'] or '정보 없음'}")
        print(f"   위치: {room['location'] or '정보 없음'}")
        print(f"   링크: {room['link'] or '정보 없음'}")
        print(f"   원본 텍스트: {room['fullText'][:100]}..." if len(room['fullText']) > 100 else f"   원본 텍스트: {room['fullText']}")
        print("-"*40)

def save_results(rooms):
    """결과를 파일로 저장"""
    if not rooms:
        print("⚠️  저장할 매물 데이터가 없습니다.")
        return
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # JSON 파일로 저장
    json_filename = f'zigbang_sidebar_{timestamp}.json'
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(rooms, f, ensure_ascii=False, indent=2)
    
    # CSV 파일로 저장
    csv_filename = f'zigbang_sidebar_{timestamp}.csv'
    fieldnames = ['index', 'title', 'price', 'area', 'floor', 'location', 'link', 'fullText']
    
    with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rooms)
    
    print(f"\n💾 결과 저장 완료:")
    print(f"   📄 JSON: {json_filename}")
    print(f"   📊 CSV: {csv_filename}")
    print(f"   📝 매물 수: {len(rooms)}개")

def main():
    """메인 실행 함수"""
    print("🚀 직방 사이드바 매물 목록 스크래핑 시작!")
    print("🎯 목표: 오른쪽 사이드바 매물 10개 수집")
    print("-"*60)
    
    try:
        scrape_zigbang_sidebar_rooms()
        print("\n✅ 스크래핑 완료!")
        
    except KeyboardInterrupt:
        print("\n⏹️  사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
    finally:
        print("\n🔚 프로그램 종료")

if __name__ == "__main__":
    main()
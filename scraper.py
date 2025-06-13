"""
scraper.py - Enhanced Modular iCal Crawler with Full v5 Capabilities
Combines the modular provider architecture with complete v5 extraction logic
"""

import asyncio
from playwright.async_api import async_playwright, Page, Frame
import pandas as pd
import re
from urllib.parse import urljoin
from typing import List, Dict, Optional, Set
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import aiohttp
import time
from dataclasses import dataclass
from bs4 import BeautifulSoup

# Import provider-specific scrapers
from apptegy_scraper import ApptegyDetector, ProviderDetectorBase, iCalResult


@dataclass
class CrawlResult:
    """Result structure for crawling operations"""
    ical_links: List[str]
    validated_link: Optional[str]
    status: str
    method: str
    error: Optional[str] = None


class EdlioDetector(ProviderDetectorBase):
    """Edlio-specific iCal link detector"""
    
    @property
    def provider_name(self) -> str:
        return "Edlio"
    
    async def can_handle(self, url: str, page_content: str = None) -> bool:
        """Detect if this is an Edlio site"""
        try:
            edlio_indicators = [
                'edlio.com',
                'powered by edlio',
                'edlio-platform',
                '/apps/events/',
                'class="ical-link"'
            ]
            
            url_lower = url.lower()
            if any(indicator in url_lower for indicator in edlio_indicators):
                return True
            
            if page_content:
                content_lower = page_content.lower()
                if any(indicator in content_lower for indicator in edlio_indicators):
                    return True
            
            return False
        except Exception:
            return False
    
    async def extract_ical_links(self, url: str, page: Page = None) -> iCalResult:
        """Extract iCal links from Edlio sites"""
        try:
            base_url = url.rstrip('/')
            if base_url.startswith('http://'):
                base_url = 'https://' + base_url.split('://', 1)[1]
            
            events_url = f"{base_url}/apps/events/"
            
            # Method 1: Direct HTTP request
            ical_links = await self._extract_edlio_via_http(events_url, base_url)
            
            # Method 2: Playwright fallback
            if not ical_links and page:
                ical_links = await self._extract_edlio_via_playwright(page, events_url, base_url)
            
            if ical_links:
                return iCalResult(
                    ical_links=ical_links,
                    provider="Edlio",
                    method="edlio_apps_events",
                    confidence=0.95,
                    status="success"
                )
            else:
                return iCalResult(
                    ical_links=[],
                    provider="Edlio",
                    method="edlio_apps_events",
                    confidence=0.0,
                    status="no_links_found"
                )
                
        except Exception as e:
            return iCalResult(
                ical_links=[],
                provider="Edlio",
                method="edlio_apps_events",
                confidence=0.0,
                status="error",
                error=str(e)
            )
    
    async def _extract_edlio_via_http(self, events_url: str, base_url: str) -> List[str]:
        """Extract using direct HTTP request"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(events_url, timeout=10) as response:
                    if response.status == 200:
                        html = await response.text()
                        return self._parse_edlio_html(html, base_url)
        except Exception:
            pass
        return []
    
    async def _extract_edlio_via_playwright(self, page: Page, events_url: str, base_url: str) -> List[str]:
        """Extract using Playwright page"""
        try:
            await page.goto(events_url, wait_until='domcontentloaded', timeout=15000)
            await page.wait_for_timeout(1000)
            
            html = await page.content()
            return self._parse_edlio_html(html, base_url)
        except Exception:
            pass
        return []
    
    def _parse_edlio_html(self, html: str, base_url: str) -> List[str]:
        """Parse HTML to find Edlio iCal links"""
        soup = BeautifulSoup(html, 'html.parser')
        ical_links = []
        
        # Primary: look for <a class="ical-link">
        ical_tag = soup.find('a', class_='ical-link', href=True)
        if ical_tag:
            href = ical_tag['href']
            href = self._normalize_edlio_url(href, base_url)
            ical_links.append(href)
        
        # Fallback: regex search for ical URLs
        if not ical_links:
            matches = re.findall(r'href=["\']([^"\']*ical[^"\']*)["\']', html, re.IGNORECASE)
            for match in matches:
                href = self._normalize_edlio_url(match, base_url)
                ical_links.append(href)
        
        return ical_links
    
    def _normalize_edlio_url(self, href: str, base_url: str) -> str:
        """Normalize Edlio URLs to HTTPS"""
        if href.startswith('webcal://'):
            href = 'https://' + href.split('://', 1)[1]
        elif href.startswith('http://'):
            href = 'https://' + href.split('://', 1)[1]
        elif href.startswith('/'):
            href = urljoin(base_url, href)
            href = href.replace('http://', 'https://')
        return href


class GenericDetector(ProviderDetectorBase):
    """Generic iCal detector with full v5 extraction capabilities"""
    
    def __init__(self):
        # Comprehensive regex patterns from v5
        self.ical_patterns = [
            r'https?://[^\s"\'<>()]+\.ics(?:\?[^\s"\'<>()]*)?',
            r'https?://calendar\.google\.com/calendar/ical/[^/\s"\'<>()]+/[^/\s"\'<>()]+\.ics',
            r'https?://[^\s"\'<>()]*[?&]format=ical[&\w=]*',
            r'https?://[^\s"\'<>()]*[?&]method=ical[&\w=]*',
            r'webcal://[^\s"\'<>()]+',
            r'https?://[^\s"\'<>()]*ical[fF]eed[^\s"\'<>()]*',
            r'https?://[^\s"\'<>()]*\.ashx[^\s"\'<>()]*calendar[^\s"\'<>()]*',
        ]
        
        # Calendar keywords for sub-page discovery
        self.calendar_keywords = [
            'calendar', 'ical', 'events', 'schedule', 'academic calendar', 
            'school calendar', 'activities', 'athletics', 'board meetings'
        ]
    
    @property
    def provider_name(self) -> str:
        return "Generic"
    
    async def can_handle(self, url: str, page_content: str = None) -> bool:
        """Generic detector can always attempt to handle any URL"""
        return True
    
    async def extract_ical_links(self, url: str, page: Page = None) -> iCalResult:
        """Extract iCal links using comprehensive v5-based methods"""
        if not page:
            return iCalResult(
                ical_links=[],
                provider="Generic",
                method="no_page_available",
                confidence=0.0,
                status="error",
                error="No page object provided"
            )
        
        try:
            # Multi-stage extraction using v5's proven approach
            all_found_links = await self._extract_all_links_generic(page, url)
            
            if all_found_links:
                # Convert Google Calendar embeds to iCal URLs
                processed_links = await self._convert_google_calendar_links(all_found_links)
                final_links = sorted(list(processed_links))
                
                return iCalResult(
                    ical_links=final_links,
                    provider="Generic",
                    method="comprehensive_extraction",
                    confidence=0.8,
                    status="success"
                )
            else:
                return iCalResult(
                    ical_links=[],
                    provider="Generic",
                    method="comprehensive_extraction",
                    confidence=0.0,
                    status="no_links_found"
                )
                
        except Exception as e:
            return iCalResult(
                ical_links=[],
                provider="Generic",
                method="comprehensive_extraction",
                confidence=0.0,
                status="error",
                error=str(e)
            )
    
    async def _extract_all_links_generic(self, page: Page, base_url: str) -> Set[str]:
        """
        Comprehensive extraction using all v5 methods in sequence
        
        Extraction Strategy (in order of preference):
        1. Quick visible link check (fastest, often sufficient)
        2. Main page comprehensive extraction
        3. Frame crawling
        4. Calendar sub-page discovery and crawling
        """
        all_found_links = set()
        current_url = page.url or base_url
        
        # Step 1: Quick visible link check (v5's most effective method)
        try:
            visible_links = await page.evaluate('''() => {
                try {
                    return Array.from(document.querySelectorAll('a[href]'))
                        .filter(a => a.offsetParent !== null)
                        .map(a => a.href)
                        .filter(href => href && (href.includes('.ics') || href.includes('ical') || href.includes('webcal') || href.includes('format=ical') || href.includes('.ashx')));
                } catch (e) { return []; }
            }''')
            
            for link in visible_links:
                if self._is_valid_ical_url(link):
                    all_found_links.add(link)
            
            # If we found links with quick check, we can often return early
            if all_found_links:
                return all_found_links
                
        except Exception:
            pass
        
        # Step 2: Comprehensive main page extraction
        try:
            main_page_links = await self._extract_all_links(page, current_url)
            all_found_links.update(main_page_links)
            
            if all_found_links:
                return all_found_links
        except Exception:
            pass
        
        # Step 3: Frame crawling (critical for embedded calendars)
        try:
            frame_links = await self._crawl_frames(page, current_url)
            all_found_links.update(frame_links)
            
            if all_found_links:
                return all_found_links
        except Exception:
            pass
        
        # Step 4: Calendar sub-page discovery and crawling
        try:
            calendar_pages = await self._find_calendar_pages(page, current_url)
            for page_info in calendar_pages[:5]:  # Limit to top 5 calendar pages
                try:
                    await page.goto(page_info['url'], wait_until='domcontentloaded', timeout=15000)
                    await page.wait_for_timeout(1000)
                    
                    subpage_links = await self._extract_all_links(page, page_info['url'])
                    if subpage_links:
                        all_found_links.update(subpage_links)
                        break  # Found links, no need to check more pages
                except Exception:
                    continue
        except Exception:
            pass
        
        return all_found_links
    
    async def _extract_all_links(self, page: Page, base_url: str) -> Set[str]:
        """Main page extraction logic from v5"""
        found_links = set()
        
        try:
            content = await page.content()
            
            # Method 1: Regex pattern matching on full content
            for pattern in self.ical_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for match in matches:
                    full_url = urljoin(base_url, match.strip())
                    if self._is_valid_ical_url(full_url):
                        found_links.add(full_url)
            
            if found_links:
                return found_links
            
            # Method 2: DOM element extraction with comprehensive selectors
            selectors = [
                'a[href*=".ics"]', 'a[href*="ical"]', 'a[href*="webcal"]', 
                'a[href*="calendar"]', 'a[href*="feed"]', 'a[href*="export"]', 
                'iframe[src*="calendar.google.com"]', 'a[href*="calendar.google.com"]'
            ]
            
            for selector in selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for element in elements[:5]:  # Limit to avoid performance issues
                        href = await element.get_attribute('href')
                        if href:
                            full_url = urljoin(base_url, href.strip())
                            if self._is_valid_ical_url(full_url):
                                found_links.add(full_url)
                        
                        # Handle Google Calendar iframes specially
                        if selector.startswith('iframe'):
                            src = await element.get_attribute('src')
                            if src and 'calendar.google.com' in src:
                                ical_url = self._extract_google_calendar_ical(src)
                                if ical_url:
                                    found_links.add(ical_url)
                        
                        if found_links:
                            return found_links
                except Exception:
                    continue
            
            # Method 3: Interactive elements check
            if not found_links:
                interactive_links = await self._check_interactive_elements(page, base_url)
                found_links.update(interactive_links)
            
            # Method 4: Text content analysis (v5 fallback method)
            if not found_links:
                try:
                    visible_text = await page.evaluate('() => document.body ? document.body.innerText || document.body.textContent || "" : ""')
                    for pattern in self.ical_patterns:
                        matches = re.findall(pattern, visible_text, re.IGNORECASE)
                        for match in matches:
                            if self._is_valid_ical_url(match):
                                found_links.add(match)
                except Exception:
                    pass
            
        except Exception:
            pass
        
        return found_links
    
    async def _crawl_frames(self, page: Page, base_url: str) -> Set[str]:
        """Frame crawling logic from v5"""
        all_links = set()
        
        try:
            frames = page.frames
            for i, frame in enumerate(frames):
                if frame == page.main_frame:
                    continue
                
                try:
                    await frame.wait_for_load_state('domcontentloaded', timeout=10000)
                    frame_url = frame.url or base_url
                    
                    try:
                        frame_content = await frame.content()
                        if len(frame_content) < 100:
                            continue
                    except Exception:
                        continue
                    
                    # Extract links from this frame using same logic as main page
                    frame_links = await self._extract_frame_links(frame, frame_url)
                    if frame_links:
                        all_links.update(frame_links)
                
                except Exception:
                    continue
        except Exception:
            pass
        
        return all_links
    
    async def _extract_frame_links(self, frame: Frame, frame_url: str) -> Set[str]:
        """Extract links from a specific frame"""
        found_links = set()
        
        try:
            frame_content = await frame.content()
            
            # Apply regex patterns to frame content
            for pattern in self.ical_patterns:
                matches = re.findall(pattern, frame_content, re.IGNORECASE)
                for match in matches:
                    full_url = urljoin(frame_url, match.strip())
                    if self._is_valid_ical_url(full_url):
                        found_links.add(full_url)
        except Exception:
            pass
        
        return found_links
    
    async def _find_calendar_pages(self, page: Page, base_url: str) -> List[Dict]:
        """Calendar page discovery logic from v5"""
        calendar_pages = []
        
        try:
            links = await page.evaluate('''() => {
                const links = [];
                document.querySelectorAll('a[href]').forEach(a => {
                    if (a.href && a.textContent) {
                        links.push({
                            href: a.href,
                            text: a.textContent.trim()
                        });
                    }
                });
                return links;
            }''')
            
            for link_data in links:
                href = link_data['href']
                text = link_data['text'].lower()
                
                if not self._is_html_url(href):
                    continue
                
                # Score based on calendar keywords
                score = sum(1 for keyword in self.calendar_keywords if keyword in text or keyword in href.lower())
                
                if score > 0:
                    calendar_pages.append({
                        'url': href, 
                        'text': link_data['text'][:50], 
                        'score': score
                    })
            
            # Sort by score (highest first) and return top candidates
            calendar_pages.sort(key=lambda x: x['score'], reverse=True)
            return calendar_pages[:5]
            
        except Exception:
            return []
    
    def _is_html_url(self, url: str) -> bool:
        """Check if URL is likely an HTML page (not a file download)"""
        if not url:
            return False
        url_lower = url.lower()
        skip_patterns = ['.pdf', '.doc', '.jpg', '.png', 'mailto:', 'tel:']
        return not any(pattern in url_lower for pattern in skip_patterns)
    
    async def _check_interactive_elements(self, page: Page, base_url: str) -> Set[str]:
        """Enhanced interactive element checking from v5 with Finalsite support"""
        found_links = set()
        
        # Comprehensive list of interactive selectors from v5
        interactive_selectors = [
            'button.wcm-calendar-icalbtn', 'button[title*="iCal" i]', 'button[title*="calendar" i]',
            'button[title*="feed" i]', 'button[title*="subscribe" i]', 'button[title*="export" i]',
            'button[title*="share" i]', 'button[class*="calendar"]', 'button[class*="ical"]',
            'button[class*="export"]', 'button[class*="share"]', 'button[class*="subscribe"]',
            'a[title*="iCal" i]', 'a[title*="calendar feed" i]', 'a[title*="subscribe" i]',
            'a[title*="export" i]', 'button:has-text("iCal")', 'button:has-text("Calendar Feed")',
            'button:has-text("Subscribe")', 'button:has-text("Export")', 'button:has-text("Share Calendar")',
            'a:has-text("iCal")', 'a:has-text("Calendar Feed")',
        ]
        
        for selector in interactive_selectors:
            try:
                elements = await page.query_selector_all(selector)
                for element in elements[:2]:  # Limit to avoid too many clicks
                    try:
                        original_content = await page.content()
                        await element.scroll_into_view_if_needed(timeout=3000)
                        await element.click(timeout=3000)
                        await asyncio.sleep(1.0)
                        
                        new_content = await page.content()
                        
                        # Look for newly appeared iCal links
                        for pattern in self.ical_patterns:
                            new_matches = set(re.findall(pattern, new_content, re.IGNORECASE))
                            old_matches = set(re.findall(pattern, original_content, re.IGNORECASE))
                            fresh_matches = new_matches - old_matches
                            
                            for match in fresh_matches:
                                full_url = urljoin(base_url, match.strip())
                                if self._is_valid_ical_url(full_url):
                                    found_links.add(full_url)
                        
                        # Check for modals that might have appeared
                        modals = await page.query_selector_all('.modal, .dialog, [role="dialog"], .wcm-modal')
                        for modal in modals[:1]:
                            if await modal.is_visible():
                                modal_text = await modal.text_content()
                                for pattern in self.ical_patterns:
                                    matches = re.findall(pattern, modal_text, re.IGNORECASE)
                                    for match in matches:
                                        if self._is_valid_ical_url(match):
                                            found_links.add(match)
                        
                        if found_links:
                            return found_links
                            
                    except Exception:
                        continue
                
                if found_links:
                    return found_links
                
            except Exception:
                continue

        # Finalsite Calendar Special Handling (v5 specific feature)
        try:
            calendar_containers = await page.query_selector_all('div.fsCalendar, div[data-calendar-module]')
            if calendar_containers:
                finalsite_selectors = [
                    'button:has-text("Subscribe")', 'button:has-text("Export")', 
                    'button:has-text("iCal")', 'button:has-text("Add to Calendar")', 
                    'a:has-text("Subscribe")', 'a:has-text("Export")', 'a:has-text("iCal")'
                ]
                
                for selector in finalsite_selectors:
                    try:
                        buttons = await page.query_selector_all(selector)
                        for btn in buttons[:2]:
                            try:
                                await btn.scroll_into_view_if_needed(timeout=2000)
                                await btn.click(timeout=3000)
                                await asyncio.sleep(1.0)

                                post_click_html = await page.content()
                                for match in re.findall(r'https?://[^\s\'"]+\.ics', post_click_html):
                                    full_url = urljoin(base_url, match)
                                    if self._is_valid_ical_url(full_url):
                                        found_links.add(full_url)
                            except Exception:
                                pass
                    except Exception:
                        pass
        except Exception:
            pass

        return found_links
    
    async def _convert_google_calendar_links(self, found_links: Set[str]) -> Set[str]:
        """Convert Google Calendar embed URLs to iCal URLs"""
        converted_links = set()
        original_links = set()
        
        for link in found_links:
            if 'calendar.google.com' in link.lower():
                ical_link = self._extract_google_calendar_ical(link)
                if ical_link:
                    converted_links.add(ical_link)
                else:
                    original_links.add(link)
            else:
                original_links.add(link)
        
        return converted_links.union(original_links)
    
    def _extract_google_calendar_ical(self, embed_url: str) -> Optional[str]:
        """Convert Google Calendar embed to iCal URL"""
        try:
            patterns = [
                r'calendar\.google\.com/calendar/embed.*[?&]src=([^&]+)',
                r'calendar\.google\.com/calendar/.*[?&]cid=([^&]+)',
                r'calendar\.google\.com/calendar/embed/([^/?]+)',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, embed_url)
                if match:
                    cal_id = match.group(1).replace('%40', '@')
                    return f"https://calendar.google.com/calendar/ical/{cal_id}/public/basic.ics"
        except Exception:
            pass
        return None
    
    def _is_valid_ical_url(self, url: str) -> bool:
        """Check if URL is a valid iCal link"""
        if not url or len(url) < 10:
            return False
        
        url_lower = url.lower().strip()
        
        if not url_lower.startswith(('http://', 'https://', 'webcal://')):
            return False
        
        # Google Calendar links are always valid
        if 'calendar.google.com' in url_lower:
            return True
        
        # Exclude false positives
        exclude_patterns = [
            'javascript:', 'mailto:', 'tel:', '.pdf', '.doc', '.jpg', '.png'
        ]
        
        if any(pattern in url_lower for pattern in exclude_patterns):
            return False
        
        # Strong iCal indicators
        strong_indicators = [
            '.ics', 'format=ical', 'method=ical', 'webcal://', 'icalfeed', '.ashx'
        ]
        
        return any(indicator in url_lower for indicator in strong_indicators)


class iCalValidator:
    """URL validator with caching and concurrent processing"""
    
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.session = None
        self._cache = {}
    
    async def _get_session(self):
        if not self.session:
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=3)
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self.session

    async def validate_url(self, url: str) -> Dict:
        if url in self._cache:
            return self._cache[url]
        
        result = {'url': url, 'is_valid': False, 'has_events': False, 'error': None}
        
        try:
            session = await self._get_session()
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.text()
                    if "BEGIN:VCALENDAR" in content and "END:VCALENDAR" in content:
                        result['is_valid'] = True
                        if "BEGIN:VEVENT" in content:
                            result['has_events'] = True
                else:
                    result['error'] = f"HTTP {response.status}"
        except asyncio.TimeoutError:
            result['error'] = "Timeout"
        except Exception as e:
            result['error'] = str(e)
        
        self._cache[url] = result
        return result

    async def validate_multiple_urls(self, urls: List[str]) -> List[Dict]:
        if not urls:
            return []
        
        semaphore = asyncio.Semaphore(3)
        
        async def validate_with_semaphore(url):
            async with semaphore:
                return await self.validate_url(url)
        
        tasks = [validate_with_semaphore(url) for url in urls[:10]]
        return await asyncio.gather(*tasks, return_exceptions=True)

    def get_best_ical_url(self, validation_results: List[Dict]) -> Optional[str]:
        valid_results = [r for r in validation_results if isinstance(r, dict) and r.get('is_valid')]
        
        with_events = [r for r in valid_results if r.get('has_events')]
        if with_events:
            return with_events[0]['url']
        
        if valid_results:
            return valid_results[0]['url']
        
        return None

    async def close_session(self):
        if self.session:
            await self.session.close()


class ModularCalendarCrawler:
    """Orchestrates multiple provider detectors"""
    
    def __init__(self):
        # Register all provider detectors
        self.detectors = [
            ApptegyDetector(),
            EdlioDetector(),
            GenericDetector()  # Always last as fallback
        ]
    
    async def detect_provider_and_extract(self, url: str, page: Page, known_provider: str = None) -> iCalResult:
        """Detect provider and extract iCal links"""
        
        # Get page content for provider detection
        try:
            page_content = await page.content()
        except Exception:
            page_content = ""
        
        # If we know the provider, try it first
        if known_provider:
            target_detector = next(
                (d for d in self.detectors if d.provider_name.lower() == known_provider.lower()),
                None
            )
            if target_detector:
                if await target_detector.can_handle(url, page_content):
                    result = await target_detector.extract_ical_links(url, page)
                    if result.status == "success":
                        return result
        
        # Try all detectors in order
        for detector in self.detectors:
            # Skip if we already tried this one
            if known_provider and detector.provider_name.lower() == known_provider.lower():
                continue
            
            try:
                if await detector.can_handle(url, page_content):
                    result = await detector.extract_ical_links(url, page)
                    
                    if result.status == "success":
                        return result
            except Exception:
                continue
        
        # If we get here, all detectors failed
        return iCalResult(
            ical_links=[],
            provider="None",
            method="all_detectors_failed",
            confidence=0.0,
            status="no_provider_success"
        )


class ModularGoogleSheetsCalendarCrawler:
    """Enhanced iCal crawler with modular provider architecture"""
    
    def __init__(self, headless: bool = True, timeout: int = 20000, credentials_path: str = None):
        self.headless = headless
        self.timeout = timeout
        self.credentials_path = credentials_path
        self.gc = None
        self.validator = iCalValidator()
        
        # Browser management
        self.playwright = None
        self.browser = None
        self.context = None

    # Add this method to your ModularGoogleSheetsCalendarCrawler class in scraper.py
# It should go right after the __init__ method

    # Add these methods to your ModularGoogleSheetsCalendarCrawler class in scraper.py

    async def setup_google_sheets(self) -> bool:
        """Setup Google Sheets connection for VM (headless)"""
        
        # Method 1: Try pre-generated OAuth token (for VM)
        token_path = './authorized_user.json'
        if os.path.exists(token_path):
            try:
                print("Using pre-generated OAuth token...")
                self.gc = gspread.oauth(authorized_user_filename=token_path)
                print("✅ Google Sheets: OAuth connection established.")
                return True
            except Exception as e:
                print(f"Pre-generated token failed: {e}")
        
        # Method 2: Service Account (if available)
        if self.credentials_path and os.path.exists(self.credentials_path):
            try:
                print(f"Trying credentials file: {self.credentials_path}")
                with open(self.credentials_path, 'r') as f:
                    cred_data = json.load(f)
                
                if cred_data.get('type') == 'service_account':
                    print("Service account detected...")
                    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
                    credentials = Credentials.from_service_account_file(self.credentials_path, scopes=scope)
                    self.gc = gspread.authorize(credentials)
                    print("✅ Google Sheets: Service Account connection established.")
                    return True
                else:
                    print("⚠️ Client credentials file cannot be used on headless VM")
                    
            except Exception as e:
                print(f"Credentials file error: {e}")
        
        print("❌ All authentication methods failed")
        print("💡 Please generate authorized_user.json on local machine and transfer to VM")
        return False

    def read_google_sheet(self, sheet_url: str, worksheet_name: str = None) -> pd.DataFrame:
        """Read Google Sheet data"""
        if not self.gc:
            return pd.DataFrame()
        
        try:
            sheet_id_match = re.search(r'/d/([^/]+)', sheet_url)
            if not sheet_id_match:
                return pd.DataFrame()
            
            sheet_id = sheet_id_match.group(1)
            sheet = self.gc.open_by_key(sheet_id)
            worksheet = sheet.worksheet(worksheet_name) if worksheet_name else sheet.get_worksheet(0)
            
            all_values = worksheet.get_all_values()
            if not all_values:
                return pd.DataFrame()
            
            headers = all_values[0]
            cleaned_headers = []
            for i, header in enumerate(headers):
                cleaned = str(header).strip() or f"Column_{i+1}"
                if cleaned in cleaned_headers:
                    cleaned = f"{cleaned}_{cleaned_headers.count(cleaned) + 1}"
                cleaned_headers.append(cleaned)
            
            df = pd.DataFrame(all_values[1:] if len(all_values) > 1 else [], columns=cleaned_headers)
            print(f"Successfully read sheet with {len(df)} rows")
            return df
            
        except Exception as e:
            print(f"Error reading Google Sheet: {e}")
            return pd.DataFrame()

    def update_single_ical_cell(self, sheet_url: str, row_num: int, ical_link: str, nces_id: str, worksheet_name: str = None) -> bool:
        """Update a single cell in Google Sheets"""
        if not self.gc:
            return False
        
        try:
            sheet_id_match = re.search(r'/d/([^/]+)', sheet_url)
            if not sheet_id_match:
                return False
            
            sheet_id = sheet_id_match.group(1)
            sheet = self.gc.open_by_key(sheet_id)
            worksheet = sheet.worksheet(worksheet_name) if worksheet_name else sheet.get_worksheet(0)
            
            cell_address = f'G{row_num}'
            worksheet.update(values=[[ical_link]], range_name=cell_address)
            print(f"Updated row {row_num} (NCES {nces_id}): {ical_link}")
            return True
        except Exception as e:
            print(f"Sheet update error: {e}")
            return False

    async def start_browser(self):
        """Start browser for crawling"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        self.context = await self.browser.new_context()
        self.context.set_default_timeout(self.timeout)

    async def close_browser(self):
        """Close browser"""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            await self.validator.close_session()
        except Exception as e:
            print(f"Error closing browser: {e}")

    async def crawl_single_district(self, district_url: str, known_provider: str = None) -> CrawlResult:
        """Crawl a single district using the modular provider approach"""
        if not self.context:
            return CrawlResult([], None, 'browser_error', 'no_browser_context')
        
        page = None
        
        try:
            page = await self.context.new_page()
            
            # Navigate to main page
            try:
                response = await page.goto(district_url, wait_until='domcontentloaded', timeout=20000)
                if response and response.status >= 400:
                    return CrawlResult([], None, 'navigation_error', f'http_status: {response.status}')
                await page.wait_for_timeout(1000)
            except Exception as e:
                return CrawlResult([], None, 'navigation_error', f'nav_error: {str(e)}')
            
            # Use modular crawler
            modular_crawler = ModularCalendarCrawler()
            result = await modular_crawler.detect_provider_and_extract(
                url=page.url,
                page=page,
                known_provider=known_provider
            )
            
            # Convert modular result to original CrawlResult format
            if result.status == "success":
                # Validate links if possible
                try:
                    validation_results = await self.validator.validate_multiple_urls(result.ical_links)
                    validated_link = self.validator.get_best_ical_url(validation_results)
                    
                    return CrawlResult(
                        ical_links=result.ical_links,
                        validated_link=validated_link,
                        status='success_found_ical' if validated_link else 'found_unvalidated_ical',
                        method=f'{result.provider}_{result.method}'
                    )
                except Exception:
                    return CrawlResult(
                        ical_links=result.ical_links,
                        validated_link=None,
                        status='found_unvalidated_ical',
                        method=f'{result.provider}_{result.method}'
                    )
            else:
                return CrawlResult(
                    ical_links=[],
                    validated_link=None,
                    status='success_no_ical_found',
                    method=f'{result.provider}_{result.method}',
                    error=result.error
                )
        
        except Exception as e:
            return CrawlResult([], None, 'crawl_error', f'modular_error: {str(e)}')
        
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

    def extract_provider_from_row(self, row_data: pd.Series, provider_col: str) -> Optional[str]:
        """Extract and normalize provider name from spreadsheet row"""
        if not provider_col or provider_col not in row_data:
            return None
        
        provider_value = str(row_data.get(provider_col, '')).strip().lower()
        
        # Map common provider variations to standard names
        provider_mappings = {
            'apptegy': 'Apptegy',
            'app tegy': 'Apptegy',
            'edlio': 'Edlio',
            'finalsite': 'Finalsite',
            'schoolwires': 'Schoolwires',
            'blackboard': 'Blackboard'
        }
        
        for key, standard_name in provider_mappings.items():
            if key in provider_value:
                return standard_name
        
        # If provider is "no provider found" or similar, return None for generic detection
        if provider_value in ['no provider found', 'unknown', 'none', 'n/a', '']:
            return None
        
        # Return original value if not in mappings (capitalize first letter)
        return provider_value.capitalize() if provider_value else None
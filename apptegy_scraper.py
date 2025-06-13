"""
apptegy_scraper.py - Apptegy-specific iCal link detector

This module implements the Apptegy extraction logic that:
1. Appends '/events' to the base URL
2. Looks for links containing 'generate_ical'
3. Returns iCal feed URLs

Based on the notebook implementation but integrated into the modular framework.
"""

import aiohttp
import asyncio
from typing import List, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from playwright.async_api import Page
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class iCalResult:
    """Standard result structure for all provider modules"""
    ical_links: List[str]
    provider: str
    method: str
    confidence: float  # 0.0 to 1.0
    status: str
    error: Optional[str] = None


class ProviderDetectorBase(ABC):
    """Base class for all provider-specific iCal detectors"""
    
    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Name of the provider this detector handles"""
        pass
    
    @abstractmethod
    async def can_handle(self, url: str, page_content: str = None) -> bool:
        """Determine if this detector can handle the given URL"""
        pass
    
    @abstractmethod
    async def extract_ical_links(self, url: str, page: Page = None) -> iCalResult:
        """Extract iCal links using provider-specific methods"""
        pass


class ApptegyDetector(ProviderDetectorBase):
    """Apptegy-specific iCal link detector"""
    
    @property
    def provider_name(self) -> str:
        return "Apptegy"
    
    async def can_handle(self, url: str, page_content: str = None) -> bool:
        """Detect if this is an Apptegy site"""
        try:
            # Check URL patterns that indicate Apptegy
            apptegy_indicators = [
                'apptegy.com',
                'thebrainery.com',
                'powered by apptegy',
                'apptegy-platform'
            ]
            
            url_lower = url.lower()
            if any(indicator in url_lower for indicator in apptegy_indicators):
                return True
            
            # Check page content if available
            if page_content:
                content_lower = page_content.lower()
                content_indicators = [
                    'powered by apptegy',
                    'apptegy-platform',
                    'apptegy.com',
                    'class="apptegy',
                    'id="apptegy',
                    'data-apptegy',
                    'apptegy-widget',
                    'apptegy-component'
                ]
                if any(indicator in content_lower for indicator in content_indicators):
                    return True
            
            return False
        except Exception:
            return False
    
    async def extract_ical_links(self, url: str, page: Page = None) -> iCalResult:
        """Extract iCal links from Apptegy sites"""
        try:
            base_url = url.rstrip('/')
            events_url = f"{base_url}/events"
            
            # Method 1: Direct HTTP request (faster, preferred method)
            ical_links = await self._extract_via_http(events_url, base_url)
            
            # Method 2: If no links found and we have a page, try Playwright
            if not ical_links and page:
                ical_links = await self._extract_via_playwright(page, events_url, base_url)
            
            if ical_links:
                return iCalResult(
                    ical_links=ical_links,
                    provider="Apptegy",
                    method="apptegy_events_page",
                    confidence=0.95,
                    status="success"
                )
            else:
                return iCalResult(
                    ical_links=[],
                    provider="Apptegy",
                    method="apptegy_events_page", 
                    confidence=0.0,
                    status="no_links_found"
                )
                
        except Exception as e:
            return iCalResult(
                ical_links=[],
                provider="Apptegy",
                method="apptegy_events_page",
                confidence=0.0,
                status="error",
                error=str(e)
            )
    
    async def _extract_via_http(self, events_url: str, base_url: str) -> List[str]:
        """Extract using direct HTTP request (faster method)"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(events_url, timeout=10) as response:
                    if response.status == 200:
                        html = await response.text()
                        return self._parse_apptegy_html(html, base_url)
        except Exception:
            pass
        return []
    
    async def _extract_via_playwright(self, page: Page, events_url: str, base_url: str) -> List[str]:
        """Extract using Playwright page (fallback method)"""
        try:
            await page.goto(events_url, wait_until='domcontentloaded', timeout=15000)
            await page.wait_for_timeout(1000)
            
            html = await page.content()
            return self._parse_apptegy_html(html, base_url)
        except Exception:
            pass
        return []
    
    def _parse_apptegy_html(self, html: str, base_url: str) -> List[str]:
        """Parse HTML to find Apptegy iCal links"""
        soup = BeautifulSoup(html, 'html.parser')
        ical_links = []
        
        # Look for links containing 'generate_ical' (primary Apptegy pattern)
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if 'generate_ical' in href:
                # Build full URL if needed
                if href.startswith('http'):
                    full_url = href
                else:
                    full_url = urljoin(base_url, href)
                ical_links.append(full_url)
        
        # Additional Apptegy patterns (backup search)
        if not ical_links:
            # Look for other common Apptegy calendar patterns
            additional_patterns = [
                'calendar/feed',
                'events/ical',
                'calendar.ics',
                'events.ics'
            ]
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if any(pattern in href.lower() for pattern in additional_patterns):
                    if href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin(base_url, href)
                    
                    # Validate it looks like an iCal URL
                    if self._is_likely_ical_url(full_url):
                        ical_links.append(full_url)
        
        return ical_links
    
    def _is_likely_ical_url(self, url: str) -> bool:
        """Check if URL is likely an iCal feed"""
        if not url:
            return False
        
        url_lower = url.lower()
        
        # Must be HTTP/HTTPS
        if not url_lower.startswith(('http://', 'https://')):
            return False
        
        # Exclude obviously non-calendar URLs
        exclude_patterns = [
            '.pdf', '.doc', '.jpg', '.png', '.gif', 
            'javascript:', 'mailto:', 'tel:'
        ]
        
        if any(pattern in url_lower for pattern in exclude_patterns):
            return False
        
        # Look for calendar indicators
        calendar_indicators = [
            '.ics', 'ical', 'calendar', 'events', 'feed',
            'generate_ical', 'export', 'subscribe'
        ]
        
        return any(indicator in url_lower for indicator in calendar_indicators)


# Standalone functions for direct usage (compatibility with notebook approach)
def extract_ical_links_sync(base_urls: List[str]) -> dict:
    """
    Synchronous wrapper for the async Apptegy extraction
    
    Args:
        base_urls: List of Apptegy website base URLs
        
    Returns:
        Dictionary mapping each base URL to its iCal links
    """
    return asyncio.run(extract_ical_links_async(base_urls))


async def extract_ical_links_async(base_urls: List[str]) -> dict:
    """
    Async version of the original notebook function
    
    Args:
        base_urls: List of Apptegy website base URLs
        
    Returns:
        Dictionary mapping each base URL to its iCal links
    """
    results = {}
    detector = ApptegyDetector()
    
    for base_url in base_urls:
        print(f"\n🔍 Processing Apptegy site: {base_url}")
        
        # Use the detector's HTTP extraction method
        events_url = base_url.rstrip('/') + '/events'
        try:
            ical_links = await detector._extract_via_http(events_url, base_url)
            results[base_url] = ical_links
            
            if ical_links:
                print(f"✅ Found {len(ical_links)} iCal links for {base_url}:")
                for link in ical_links:
                    print(f"   {link}")
            else:
                print(f"❌ No iCal links found for {base_url}")
                
        except Exception as e:
            print(f"❌ Error processing {base_url}: {e}")
            results[base_url] = []
    
    return results


# Example usage (compatible with notebook)
async def test_apptegy_extraction():
    """Test function to verify Apptegy extraction works"""
    
    # Test URLs from the notebook
    test_urls = [
        "https://dcps.duvalschools.org",
        "https://www.dpsk12.org/",
        "https://www.aacps.org/"
    ]
    
    print("🧪 Testing Apptegy iCal Extraction")
    print("=" * 50)
    
    results = await extract_ical_links_async(test_urls)
    
    print("\n📊 Summary:")
    total_links = sum(len(links) for links in results.values())
    successful_sites = sum(1 for links in results.values() if links)
    
    print(f"   Sites processed: {len(test_urls)}")
    print(f"   Successful sites: {successful_sites}")
    print(f"   Total iCal links found: {total_links}")
    
    return results


if __name__ == "__main__":
    """
    Direct execution for testing
    """
    print("Apptegy iCal Scraper - Direct Test")
    print("=" * 40)
    
    # Run the test
    try:
        asyncio.run(test_apptegy_extraction())
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
    except Exception as e:
        print(f"\n💥 Test failed: {e}")
        import traceback
        traceback.print_exc()
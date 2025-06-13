"""
main.py - Main execution script for Modular iCal Crawler v5.0 with Multiprocessing
"""

import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import time
from typing import List
from scraper import ModularGoogleSheetsCalendarCrawler


def process_districts_batch(batch_data):
    """
    Process a batch of districts in a separate process
    
    Args:
        batch_data: Tuple containing (districts_list, config_dict)
    """
    districts_list, config = batch_data
    results = []
    
    try:
        # Create event loop for this process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def process_batch_async():
            # Create crawler instance for this process
            crawler = ModularGoogleSheetsCalendarCrawler(
                headless=config['headless'],
                credentials_path=config['credentials_path'],
                timeout=config['timeout']
            )
            
            # Start browser
            await crawler.start_browser()
            
            batch_results = []
            try:
                for district_data in districts_list:
                    (original_index, nces_id, url_to_crawl, 
                     sheet_row_num, known_provider) = district_data
                    
                    if not url_to_crawl or url_to_crawl.lower() in ['nan', 'none', 'na']:
                        result = {
                            'original_index': original_index,
                            'nces_id': nces_id,
                            'sheet_row_num': sheet_row_num,
                            'status': 'invalid_url',
                            'method': 'invalid_url',
                            'ical_links': [],
                            'validated_link': None,
                            'error': 'Invalid or empty URL'
                        }
                        batch_results.append(result)
                        continue
                    
                    try:
                        # Crawl the district
                        crawl_result = await crawler.crawl_single_district(url_to_crawl, known_provider)
                        
                        result = {
                            'original_index': original_index,
                            'nces_id': nces_id,
                            'sheet_row_num': sheet_row_num,
                            'status': crawl_result.status,
                            'method': crawl_result.method,
                            'ical_links': crawl_result.ical_links,
                            'validated_link': crawl_result.validated_link,
                            'error': crawl_result.error
                        }
                        batch_results.append(result)
                        
                    except Exception as e:
                        result = {
                            'original_index': original_index,
                            'nces_id': nces_id,
                            'sheet_row_num': sheet_row_num,
                            'status': 'crawl_error',
                            'method': 'process_error',
                            'ical_links': [],
                            'validated_link': None,
                            'error': f'Process error: {str(e)}'
                        }
                        batch_results.append(result)
            
            finally:
                # Clean up browser
                await crawler.close_browser()
            
            return batch_results
        
        # Run the async function
        results = loop.run_until_complete(process_batch_async())
        loop.close()
        
    except Exception as e:
        # If batch fails completely, create error results for all districts
        for district_data in districts_list:
            (original_index, nces_id, url_to_crawl, 
             sheet_row_num, known_provider) = district_data
            
            result = {
                'original_index': original_index,
                'nces_id': nces_id,
                'sheet_row_num': sheet_row_num,
                'status': 'batch_error',
                'method': 'batch_error',
                'ical_links': [],
                'validated_link': None,
                'error': f'Batch processing error: {str(e)}'
            }
            results.append(result)
    
    return results


class MultiprocessingController:
    """Controls multiprocessing execution and Google Sheets updates"""
    
    def __init__(self, crawler: ModularGoogleSheetsCalendarCrawler):
        self.crawler = crawler
    
    async def process_with_multiprocessing(self, sheet_url: str, start_row: int = 1, 
                                         end_row: int = None, worksheet_name: str = None,
                                         target_providers: List[str] = None,
                                         num_processes: int = 4, districts_per_batch: int = 10):
        """Process districts using multiprocessing with real-time Google Sheets updates"""
        
        print(f"🚀 Starting multiprocessing crawl with {num_processes} processes")
        print(f"📦 Batch size: {districts_per_batch} districts per batch")
        
        # Setup Google Sheets
        if not await self.crawler.setup_google_sheets():
            print("❌ Failed to setup Google Sheets")
            return False
        
        # Read and filter data
        df_input = self.crawler.read_google_sheet(sheet_url, worksheet_name)
        if df_input.empty:
            print("❌ Failed to read Google Sheet or sheet is empty")
            return False
        
        # Column mapping
        cols = {c.lower().strip(): c for c in df_input.columns}
        nces_col = cols.get('nces id', cols.get('nces_id', cols.get('nces', df_input.columns[0] if len(df_input.columns) > 0 else None)))
        url_col = cols.get('link', cols.get('url', df_input.columns[1] if len(df_input.columns) > 1 else None))
        status_col = cols.get('test result', cols.get('status', cols.get('pass', df_input.columns[2] if len(df_input.columns) > 2 else None)))
        provider_col = cols.get('provider', df_input.columns[4] if len(df_input.columns) > 4 else None)
        
        # Filter data
        actual_start_index = max(0, start_row - 1)
        actual_end_index = (end_row - 1) if end_row and end_row > 0 else len(df_input)
        df_to_process = df_input.iloc[actual_start_index:actual_end_index].copy()
        
        # Filter for 'pass' status only
        df_to_process = df_to_process[
            df_to_process[status_col].astype(str).str.lower().str.strip() == 'pass'
        ]
        
        # Filter by target providers if specified
        if target_providers and provider_col:
            provider_filter = df_to_process[provider_col].astype(str).str.lower().str.strip()
            target_providers_lower = [p.lower() for p in target_providers]
            
            if 'generic' in target_providers_lower:
                target_providers_lower.extend(['no provider found', 'unknown', 'none', 'n/a', ''])
            
            df_to_process = df_to_process[provider_filter.isin(target_providers_lower)]
        
        if df_to_process.empty:
            print("❌ No rows match the filtering criteria")
            return False
        
        total_districts = len(df_to_process)
        print(f"📊 Processing {total_districts} districts")
        
        # Prepare data for multiprocessing
        district_data_list = []
        for df_idx, row_data in df_to_process.iterrows():
            sheet_row_num = df_idx + 2
            nces_id = str(row_data.get(nces_col, '')).strip()
            url_to_crawl = str(row_data.get(url_col, '')).strip()
                    # Extract provider information
            known_provider = self.crawler.extract_provider_from_row(row_data, provider_col) if provider_col else None
            
            district_data_list.append((
                len(district_data_list),  # original_index
                nces_id,
                url_to_crawl,
                sheet_row_num,
                known_provider
            ))
        
        # Create batches
        batches = [
            district_data_list[i:i + districts_per_batch] 
            for i in range(0, len(district_data_list), districts_per_batch)
        ]
        num_batches = len(batches)
        
        print(f"📦 Created {num_batches} batches")
        
        # Prepare config for worker processes
        config = {
            'headless': self.crawler.headless,
            'credentials_path': self.crawler.credentials_path,
            'timeout': self.crawler.timeout
        }
        
        # Prepare batch data for multiprocessing
        batch_data = [(batch, config) for batch in batches]
        
        # Track progress
        all_results = []
        success_count = 0
        start_time = time.time()
        
        # Process batches with multiprocessing
        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            # Submit all batches
            futures = [executor.submit(process_districts_batch, data) for data in batch_data]
            
            # Process completed batches
            for i, future in enumerate(futures):
                try:
                    # Get batch results with timeout
                    current_batch_size = len(batches[i])
                    timeout_for_batch = (current_batch_size * 120) + 300
                    
                    batch_results = future.result(timeout=timeout_for_batch)
                    all_results.extend(batch_results)
                    
                    # Update Google Sheets in real-time
                    batch_success_count = 0
                    for result in batch_results:
                        try:
                            # Determine update value based on result
                            if result['status'] == 'success_found_ical':
                                update_value = result['validated_link'] or (result['ical_links'][0] if result['ical_links'] else 'Found but no URL')
                                batch_success_count += 1
                                print(f"✅ SUCCESS {result['nces_id']}: {result['method']}")
                            elif result['status'] == 'found_unvalidated_ical':
                                update_value = f"Found but unvalidated: {result['ical_links'][0] if result['ical_links'] else 'Unknown'}"
                                print(f"⚠️ UNVALIDATED {result['nces_id']}: {result['method']}")
                            elif result['status'] == 'success_no_ical_found':
                                update_value = "No iCal found"
                                print(f"➖ NO ICAL {result['nces_id']}: {result['method']}")
                            else:
                                update_value = f"Error: {result['error'] or result['status']}"
                                print(f"❌ ERROR {result['nces_id']}: {result['error'] or result['status']}")
                            
                            # Update the sheet
                            self.crawler.update_single_ical_cell(
                                sheet_url, 
                                result['sheet_row_num'], 
                                update_value, 
                                result['nces_id'], 
                                worksheet_name
                            )
                            
                        except Exception as update_error:
                            print(f"⚠️ Failed to update sheet for {result['nces_id']}: {update_error}")
                    
                    success_count += batch_success_count
                    completed_districts = min((i + 1) * districts_per_batch, total_districts)
                    elapsed = time.time() - start_time
                    rate = completed_districts / elapsed * 60 if elapsed > 0 else 0
                    success_rate = (success_count / completed_districts) * 100 if completed_districts > 0 else 0
                    
                    print(f"🔄 Batch {i + 1}/{num_batches} complete: {completed_districts}/{total_districts} districts")
                    print(f"   Success: {success_rate:.1f}% | Rate: {rate:.1f}/min | Batch: {batch_success_count}/{current_batch_size}")
                    
                except Exception as e:
                    print(f"❌ Batch {i + 1} failed: {e}")
                    
                    # Update failed districts in sheet
                    failed_batch = batches[i]
                    for district_data in failed_batch:
                        try:
                            (original_index, nces_id, url_to_crawl, 
                             sheet_row_num, known_provider) = district_data
                            
                            self.crawler.update_single_ical_cell(
                                sheet_url,
                                sheet_row_num,
                                f"Batch Error: {type(e).__name__}",
                                nces_id,
                                worksheet_name
                            )
                        except Exception:
                            pass
        
        # Final statistics
        elapsed = time.time() - start_time
        success_rate = (success_count / len(all_results)) * 100 if all_results else 0
        
        print(f"\n🎯 FINAL RESULTS:")
        print(f"📊 Processed: {len(all_results)} districts")
        print(f"✅ Successful: {success_count} ({success_rate:.1f}%)")
        print(f"⏱️ Total time: {elapsed/60:.1f} minutes")
        print(f"🚀 Average rate: {len(all_results)/(elapsed/60):.1f} districts/minute")
        
        return True


async def main():
    """Main execution function with provider targeting"""
    
    # Configuration
    GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1Ip2kTmDkLbSkPiCm7F5XBqbSKLI6TpjINoYFxOGYeFY/edit?gid=1033503817#gid=1033503817"
    START_ROW = 1
    END_ROW = 6  # Process all rows
    CREDENTIALS_FILE = "icalcredentials.json"
    WORKSHEET_NAME = "Version1 with ical link"
    HEADLESS = True
    
    # Multiprocessing configuration
    NUM_PROCESSES = 6
    DISTRICTS_PER_BATCH = 8
    
    # Provider targeting
    TARGET_PROVIDERS = None # Change to ["Edlio"], ["Apptegy", "Edlio"], ["Generic"], or None for all
    
    print("🚀 iCal Crawler v5.0 - Modular Architecture with Multiprocessing")
    print("=" * 70)
    print("Features:")
    print("✓ Modular provider detection (Apptegy, Edlio, Generic)")
    print("✓ Multiprocessing for 6x speed improvement")
    print("✓ Real-time Google Sheets updates")
    print("✓ Provider-specific extraction methods")
    print("✓ Intelligent fallback strategies")
    print("✓ Batch progress tracking")
    
    print(f"⚙️ Processes: {NUM_PROCESSES} | Batch size: {DISTRICTS_PER_BATCH}")
    
    if TARGET_PROVIDERS:
        print(f"🎯 Targeting providers: {', '.join(TARGET_PROVIDERS)}")
    else:
        print("🌐 Processing all providers")
    
    print("=" * 70)
    
    # Create crawler instance
    crawler = ModularGoogleSheetsCalendarCrawler(
        headless=HEADLESS,
        credentials_path=CREDENTIALS_FILE,
        timeout=20000
    )
    
    # Create multiprocessing controller
    mp_controller = MultiprocessingController(crawler)
    
    # Process with multiprocessing
    success = await mp_controller.process_with_multiprocessing(
        sheet_url=GOOGLE_SHEET_URL,
        start_row=START_ROW,
        end_row=END_ROW,
        worksheet_name=WORKSHEET_NAME,
        target_providers=TARGET_PROVIDERS,
        num_processes=NUM_PROCESSES,
        districts_per_batch=DISTRICTS_PER_BATCH
    )
    
    if success:
        print("\n✅ Processing completed successfully!")
    else:
        print("\n❌ Processing failed!")


async def quick_run_examples():
    """Quick run examples for testing different configurations"""
    
    configs = {
        "apptegy": ["Apptegy"],
        "edlio": ["Edlio"], 
        "unknown": ["Generic"],  # "no provider found" entries
        "all": None
    }
    
    print("\nQuick Run Examples:")
    print("1. apptegy - Process only Apptegy districts")
    print("2. edlio - Process only Edlio districts") 
    print("3. unknown - Process districts with 'no provider found'")
    print("4. all - Process all districts")
    
    choice = input("\nEnter choice (apptegy/edlio/unknown/all) or press Enter to use main config: ").strip().lower()
    
    if choice in configs:
        target_providers = configs[choice]
        print(f"Running with target providers: {target_providers}")
        
        crawler = ModularGoogleSheetsCalendarCrawler(
            headless=True,
            credentials_path="icalcredentials.json"
        )
        
        mp_controller = MultiprocessingController(crawler)
        
        success = await mp_controller.process_with_multiprocessing(
            sheet_url="https://docs.google.com/spreadsheets/d/1Ip2kTmDkLbSkPiCm7F5XBqbSKLI6TpjINoYFxOGYeFY/edit?gid=1033503817#gid=1033503817",
            start_row=1,
            end_row=50,  # Limit to 50 for testing
            worksheet_name="Version1 with ical link",
            target_providers=target_providers,
            num_processes=4,  # Fewer processes for testing
            districts_per_batch=5
        )
        
        return success
    else:
        print("Using main configuration...")
        return await main()


async def test_single_url():
    """Test the system on a single URL"""
    
    test_urls = {
        "1": ("https://dcps.duvalschools.org", "Apptegy"),
        "2": ("https://www.dpsk12.org/", "Apptegy"),
        "3": ("https://www.aacps.org/", "Apptegy"),
        "4": ("https://www.kernhigh.org", "Edlio"),
        "5": ("https://www.lausd.net", None)
    }
    
    print("\nTest Single URL:")
    for key, (url, provider) in test_urls.items():
        print(f"{key}. {url} ({provider or 'Unknown'})")
    
    choice = input("\nEnter number (1-5): ").strip()
    
    if choice in test_urls:
        url, known_provider = test_urls[choice]
        print(f"\nTesting {url} (Provider: {known_provider or 'Unknown'})")
        
        crawler = ModularGoogleSheetsCalendarCrawler()
        await crawler.start_browser()
        
        try:
            result = await crawler.crawl_single_district(url, known_provider)
            
            print(f"  Status: {result.status}")
            print(f"  Method: {result.method}")
            print(f"  Links found: {len(result.ical_links)}")
            if result.ical_links:
                for i, link in enumerate(result.ical_links[:3], 1):
                    print(f"    {i}. {link}")
            if result.validated_link:
                print(f"  ✅ Validated: {result.validated_link}")
            
        finally:
            await crawler.close_browser()
    else:
        print("Invalid choice")


if __name__ == "__main__":
    multiprocessing.freeze_support()  # Required for Windows
    
    try:
        # Start processing immediately
        asyncio.run(main())
            
    except KeyboardInterrupt:
        print("\n🛑 Process interrupted by user")
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()
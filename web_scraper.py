import re
import csv
import time
import os
import platform
import subprocess
import concurrent.futures
import pandas as pd
from urllib.parse import urlparse
import requests
import warnings
import json
import zipfile
import shutil
from bs4 import BeautifulSoup
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import TimeoutException, WebDriverException

# Try to import dotenv, but don't fail if not available
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    pass  # dotenv not installed, continue without it

# Suppress the InsecureRequestWarning from urllib3
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class CompanyScraper:
    def __init__(self, timeout=20, use_selenium=True, retry_count=3, debug=False, force_driver_update=False):
        self.timeout = timeout
        self.use_selenium = use_selenium
        self.retry_count = retry_count
        self.debug = debug
        self.force_driver_update = force_driver_update
        
        # Set the path for ChromeDriver relative to the script
        self.driver_dir = os.path.dirname(__file__)
        self.chrome_driver_path = os.path.join(self.driver_dir, 'chromedriver')
        if platform.system().lower() == "windows":
            self.chrome_driver_path += '.exe'
        
        # Regular expressions for extracting data
        self.phone_pattern = re.compile(r'(\+?[\d\s\-\(\)]{8,20})', re.MULTILINE)
        
        # Social media patterns
        self.social_media_patterns = {
            'facebook': re.compile(r'(facebook\.com/[A-Za-z0-9\.\_\-]+)'),
            'twitter': re.compile(r'(twitter\.com/[A-Za-z0-9\_]+)'),
            'instagram': re.compile(r'(instagram\.com/[A-Za-z0-9\.\_\-]+)'),
            'linkedin': re.compile(r'(linkedin\.com/(?:company|in)/[A-Za-z0-9\-\_\.]+)'),
            'youtube': re.compile(r'(youtube\.com/(?:user|channel)/[A-Za-z0-9\-\_\.]+)')
        }
        
        # Address pattern (this is simplistic; real-world addresses vary greatly)
        self.address_pattern = re.compile(r'(\d+\s+[A-Za-z\s\,\.]+(?:Avenue|Lane|Road|Boulevard|Drive|Street|Ave|Ln|Rd|Blvd|Dr|St)[\,\s\.]+[A-Za-z\s]+\,\s*[A-Z]{2}\s*\d{5})')
        
        # Add Chrome binary location option
        self.chrome_binary_path = os.getenv('CHROME_BINARY_PATH')
        
        # Initialize Selenium if enabled
        if self.use_selenium:
            self.ensure_chromedriver()

    def ensure_chromedriver(self):
        """Ensure a compatible ChromeDriver is available, download if needed"""
        chrome_version = self._get_chrome_version()
        if not chrome_version:
            print("Warning: Could not detect Chrome version. Downloading latest ChromeDriver.")
            chrome_major_version = None
        else:
            chrome_major_version = chrome_version.split('.')[0]
            print(f"Detected Chrome version: {chrome_version} (major version: {chrome_major_version})")
        
        # Check if we need to download a new ChromeDriver
        download_needed = True
        if os.path.exists(self.chrome_driver_path) and not self.force_driver_update:
            try:
                # Check existing chromedriver version
                version_cmd = [self.chrome_driver_path, "--version"]
                driver_version_output = subprocess.check_output(version_cmd).decode('utf-8')
                driver_version_match = re.search(r'ChromeDriver (\d+)', driver_version_output)
                
                if driver_version_match:
                    driver_major_version = driver_version_match.group(1)
                    print(f"Existing ChromeDriver major version: {driver_major_version}")
                    
                    # If versions match, no need to download
                    if chrome_major_version and driver_major_version == chrome_major_version:
                        print("Existing ChromeDriver is compatible with current Chrome version.")
                        download_needed = False
            except Exception as e:
                print(f"Could not check existing ChromeDriver version: {e}")
        
        if download_needed:
            print(f"Downloading compatible ChromeDriver for Chrome {chrome_major_version}...")
            
            # Remove existing driver if it exists
            if os.path.exists(self.chrome_driver_path):
                try:
                    os.remove(self.chrome_driver_path)
                    print(f"Removed existing ChromeDriver at {self.chrome_driver_path}")
                except Exception as e:
                    print(f"Warning: Could not remove existing ChromeDriver: {e}")
            
            # Download appropriate ChromeDriver version
            download_url = self._get_chromedriver_download_url(chrome_major_version)
            if download_url:
                self._download_and_extract_chromedriver(download_url)
            else:
                print("Error: Could not determine appropriate ChromeDriver download URL.")
                if os.path.exists(self.chrome_driver_path):
                    print("Will attempt to use existing ChromeDriver, but it may not be compatible.")
        
        # Verify ChromeDriver exists and is executable
        if os.path.exists(self.chrome_driver_path):
            if platform.system().lower() != "windows":
                os.chmod(self.chrome_driver_path, 0o755)  # Make executable on Unix-like systems
            print(f"ChromeDriver is ready at {self.chrome_driver_path}")
        else:
            print("Error: ChromeDriver is not available. Web scraping with Selenium will fail.")

    def _get_chromedriver_download_url(self, chrome_major_version):
        """Get the appropriate ChromeDriver download URL based on Chrome version"""
        try:
            # If we have a specific Chrome version, get the corresponding driver
            if chrome_major_version:
                url = f"https://chromedriver.storage.googleapis.com/LATEST_RELEASE_{chrome_major_version}"
                response = requests.get(url)
                if response.status_code == 200:
                    driver_version = response.text.strip()
                    print(f"Found ChromeDriver version {driver_version} for Chrome {chrome_major_version}")
                else:
                    # If we can't find a specific version match, fall back to latest
                    print(f"No specific ChromeDriver for Chrome {chrome_major_version}, using latest")
                    response = requests.get("https://chromedriver.storage.googleapis.com/LATEST_RELEASE")
                    if response.status_code == 200:
                        driver_version = response.text.strip()
                        print(f"Using latest ChromeDriver version: {driver_version}")
                    else:
                        print("Error: Could not determine latest ChromeDriver version")
                        return None
            else:
                # If we don't know Chrome version, get the latest driver
                response = requests.get("https://chromedriver.storage.googleapis.com/LATEST_RELEASE")
                if response.status_code == 200:
                    driver_version = response.text.strip()
                    print(f"Using latest ChromeDriver version: {driver_version}")
                else:
                    print("Error: Could not determine latest ChromeDriver version")
                    return None
            
            # Determine platform
            system = platform.system().lower()
            arch = platform.machine().lower()
            
            if system == "windows":
                platform_name = "win32"
            elif system == "darwin":  # macOS
                if "arm" in arch or "aarch64" in arch:
                    platform_name = "mac_arm64"
                else:
                    platform_name = "mac64"
            elif system == "linux":
                if "arm" in arch or "aarch64" in arch:
                    platform_name = "linux64_arm"
                else:
                    platform_name = "linux64"
            else:
                raise Exception(f"Unsupported system: {system}")
            
            download_url = f"https://chromedriver.storage.googleapis.com/{driver_version}/chromedriver_{platform_name}.zip"
            print(f"ChromeDriver download URL: {download_url}")
            return download_url
            
        except Exception as e:
            print(f"Error determining ChromeDriver download URL: {e}")
            return None
    
    def _download_and_extract_chromedriver(self, download_url):
        """Download and extract ChromeDriver"""
        try:
            # Download the ChromeDriver zip file
            print(f"Downloading ChromeDriver from {download_url}...")
            response = requests.get(download_url)
            if response.status_code != 200:
                print(f"Error downloading ChromeDriver: HTTP {response.status_code}")
                return False
            
            # Save to a temporary zip file
            zip_path = os.path.join(self.driver_dir, 'chromedriver_temp.zip')
            with open(zip_path, 'wb') as f:
                f.write(response.content)
            
            # Extract the zip file
            print(f"Extracting ChromeDriver...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(self.driver_dir)
            
            # Move the extracted ChromeDriver to the correct location if needed
            extracted_name = 'chromedriver'
            if platform.system().lower() == "windows":
                extracted_name += '.exe'
            
            extracted_path = os.path.join(self.driver_dir, extracted_name)
            if os.path.exists(extracted_path) and extracted_path != self.chrome_driver_path:
                shutil.move(extracted_path, self.chrome_driver_path)
            
            # Clean up
            os.remove(zip_path)
            print(f"ChromeDriver successfully installed at {self.chrome_driver_path}")
            
            # Set permissions on Unix-like systems
            if platform.system().lower() != "windows":
                os.chmod(self.chrome_driver_path, 0o755)
            
            return True
        
        except Exception as e:
            print(f"Error installing ChromeDriver: {e}")
            return False
    
    def _get_chrome_version(self):
        """Get the installed Chrome version"""
        try:
            # Check for Chrome binary path first
            chrome_path = self.chrome_binary_path
            
            # If not specified, try to find Chrome in standard locations
            if not chrome_path or not os.path.exists(chrome_path):
                if platform.system().lower() == "darwin":  # macOS
                    chrome_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
                elif platform.system().lower() == "windows":
                    # Try common Windows locations
                    possible_paths = [
                        os.path.expanduser("~\\AppData\\Local\\Google\\Chrome\\Application\\chrome.exe"),
                        "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
                        "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
                    ]
                    for path in possible_paths:
                        if os.path.exists(path):
                            chrome_path = path
                            break
                else:  # Linux
                    # Try common commands
                    for cmd in ["google-chrome", "chrome", "chromium", "chromium-browser"]:
                        try:
                            chrome_path = subprocess.check_output(["which", cmd], stderr=subprocess.STDOUT).decode().strip()
                            if chrome_path:
                                break
                        except:
                            continue
            
            # If we found a path, try to get the version
            if chrome_path and os.path.exists(chrome_path):
                try:
                    version_output = subprocess.check_output([chrome_path, "--version"], stderr=subprocess.STDOUT).decode()
                    # Extract the version number
                    match = re.search(r'(\d+\.\d+\.\d+\.\d+)', version_output)
                    if match:
                        return match.group(1)
                except:
                    pass
            
            # Fallback to registry on Windows
            if platform.system().lower() == "windows":
                try:
                    import winreg
                    for key_path in [
                        r"SOFTWARE\Google\Chrome\BLBeacon",
                        r"SOFTWARE\Wow6432Node\Google\Chrome\BLBeacon"
                    ]:
                        try:
                            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path) as key:
                                version = winreg.QueryValueEx(key, "version")[0]
                                return version
                        except:
                            continue
                except:
                    pass
            
            # If we get here, we couldn't find the Chrome version
            return None
            
        except Exception as e:
            print(f"Error detecting Chrome version: {e}")
            return None

    def get_page_content(self, url):
        """Get page content using requests first, then fallback to Chrome"""
        # Try requests strategy first
        content = self._requests_strategy(url)
        if content and len(content) > 700:
            return content

        # If requests failed, use Chrome
        print(f"Requests strategy failed for {url}, falling back to Chrome")
        return self._chrome_strategy(url)
        
    def _chrome_strategy(self, url):
        """Use Chrome for scraping with optimized settings"""
        driver = None
        try:
            print(f"Trying Chrome strategy for {url}...")
            options = ChromeOptions()
            options.add_argument('--headless=new')  
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-popup-blocking')
            options.add_argument('--disable-notifications')
            options.add_argument('--blink-settings=imagesEnabled=false')
            options.add_argument('--disable-browser-side-navigation')
            
            # Add custom binary if specified
            if self.chrome_binary_path and os.path.exists(self.chrome_binary_path):
                options.binary_location = self.chrome_binary_path
            
            # Add user agent
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36')
            
            # Different page load strategies
            # options.page_load_strategy = 'normal'  # Default
            options.page_load_strategy = 'eager'  # DOM access ready, but resources still loading
            
            # Create Chrome driver
            service = ChromeService(executable_path=self.chrome_driver_path)
            driver = webdriver.Chrome(service=service, options=options)
            
            # Set page load timeout
            driver.set_page_load_timeout(self.timeout)
            
            try:
                driver.get(url)
            except TimeoutException as e:
                print(f"Chrome page load timeout for {url}: {e}")
                # Continue to try to get partial content
            except Exception as e:
                print(f"Chrome navigation issue for {url}: {e}")
                # Continue to try to get partial content
                
            # Wait a moment for content to load
            time.sleep(min(2, self.timeout // 5))
            
            # Get page source
            page_source = driver.page_source
            
            # Check if we got meaningful content
            if page_source and len(page_source) > 1000:
                return page_source
            else:
                print(f"Chrome retrieved insufficient content for {url}")
                return None
            
        except Exception as e:
            print(f"Chrome strategy failed for {url}: {e}")
            return None
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    def _requests_strategy(self, url):
        """Fallback method using requests library"""
        try:
            print(f"Trying fallback with requests for {url}...")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }
            
            # Try with a shorter timeout first
            short_timeout = min(5, self.timeout // 2)
            try:
                response = requests.get(url, headers=headers, timeout=short_timeout, allow_redirects=True)
                response.raise_for_status()
                return response.text
            except (requests.Timeout, requests.ConnectionError) as e:
                print(f"Short timeout failed for {url}, trying with full timeout: {e}")
            
            # If short timeout failed, try with full timeout
            response = requests.get(url, headers=headers, timeout=self.timeout, allow_redirects=True)
            response.raise_for_status()
            return response.text
            
        except requests.exceptions.SSLError as e:
            print(f"SSL Error for {url}, trying without verification: {e}")
            try:
                # Try again without SSL verification as a last resort
                response = requests.get(url, headers=headers, timeout=self.timeout, verify=False, allow_redirects=True)
                response.raise_for_status()
                return response.text
            except Exception as inner_e:
                print(f"Failed even without SSL verification for {url}: {inner_e}")
                return None
        except Exception as e:
            print(f"Requests error for {url}: {e}")
            return None

    def extract_phones(self, text):
        """Extract phone numbers from text"""
        if not text:
            return []
            
        phones = self.phone_pattern.findall(text)
        # Clean up and standardize phone numbers
        cleaned_phones = []
        for phone in phones:
            # Remove non-numeric characters except + at the beginning
            cleaned = re.sub(r'[^\d+]', '', phone)
            if len(cleaned) >= 8:  # Minimum length for valid phone number
                cleaned_phones.append(cleaned)
        return list(set(cleaned_phones))  # Remove duplicates

    def extract_social_media(self, text):
        """Extract social media links from text"""
        if not text:
            return {}
            
        social_media = {}
        for platform, pattern in self.social_media_patterns.items():
            matches = pattern.findall(text)
            if matches:
                social_media[platform] = list(set(matches))  # Remove duplicates
        return social_media

    def extract_address(self, text):
        """Extract address from text"""
        if not text:
            return []
            
        addresses = self.address_pattern.findall(text)
        return list(set(addresses))  # Remove duplicates

    def scrape_website(self, url):
        """Scrape a website for company information with retries"""
        # Initialize retry counter
        retries = 0
        last_error = None
        
        while retries <= self.retry_count:
            try:
                # Add http:// prefix if missing
                if not url.startswith(('http://', 'https://')):
                    url = 'http://' + url
                    
                # Parse domain for later use
                domain = urlparse(url).netloc
                
                if retries > 0:
                    print(f"Retry #{retries} for {url}")
                
                # Fetch the website content
                page_content = self.get_page_content(url)
                
                if not page_content:
                    last_error = "Failed to retrieve page content"
                    retries += 1
                    time.sleep(2)  # Wait before retry
                    continue
                
                # Parse HTML
                soup = BeautifulSoup(page_content, 'html.parser')
                
                # Get all text content
                text_content = soup.get_text()
                
                # Extract data
                phones = self.extract_phones(text_content)
                social_media = self.extract_social_media(page_content)
                addresses = self.extract_address(text_content)
                
                # Prepare result
                result = {
                    'website': url,
                    'domain': domain,
                    'phones': phones,
                    'addresses': addresses,
                    'status': 'success',
                    'retries': retries
                }
                
                # Add social media links
                for platform, links in social_media.items():
                    result[f'{platform}_links'] = links
                    
                return result
                
            except Exception as e:
                last_error = str(e)
                retries += 1
                
                if self.debug:
                    print(f"Error on attempt {retries} for {url}: {last_error}")
                
                # Wait before retry with exponential backoff
                if retries <= self.retry_count:
                    time.sleep(2 * retries)
        
        # All retries failed
        return {
            'website': url,
            'status': 'failed',
            'error': last_error,
            'retries': retries - 1
        }

    def batch_scrape(self, urls, max_workers=10):
        """Scrape multiple websites in parallel"""
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit scraping tasks
            future_to_url = {executor.submit(self.scrape_website, url): url for url in urls}
            
            # Process results as they complete
            for future in tqdm(concurrent.futures.as_completed(future_to_url), total=len(urls)):
                url = future_to_url[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append({
                        'website': url,
                        'status': 'failed',
                        'error': str(e),
                        'retries': -1
                    })
        
        return results

def load_websites(csv_file):
    """Load websites from CSV file"""
    websites = []
    with open(csv_file, 'r') as f:
        reader = csv.reader(f)
        # Skip header if exists
        header = next(reader, None)
        for row in reader:
            if row and len(row) > 0 and row[0].strip():
                websites.append(row[0].strip())
    return websites

def save_results(results, output_file):
    """Save scraping results to CSV"""
    # Convert results to DataFrame
    df = pd.DataFrame(results)
    
    # Convert list fields to string for CSV storage
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Results saved to {output_file}")

def analyze_results(results):
    """Analyze scraping results"""
    total_websites = len(results)
    successful = sum(1 for r in results if r['status'] == 'success')
    
    # Calculate coverage
    coverage = (successful / total_websites) * 100 if total_websites > 0 else 0
    
    # Calculate fill rates for successful scrapes
    fill_rates = {}
    if successful > 0:
        # Phone numbers
        with_phones = sum(1 for r in results if r['status'] == 'success' and r.get('phones') and len(r['phones']) > 0)
        fill_rates['phones'] = (with_phones / successful) * 100
        
        # Social media
        social_platforms = ['facebook_links', 'twitter_links', 'instagram_links', 'linkedin_links', 'youtube_links']
        for platform in social_platforms:
            with_platform = sum(1 for r in results if r['status'] == 'success' and r.get(platform) and len(r[platform]) > 0)
            fill_rates[platform] = (with_platform / successful) * 100
        
        # Addresses
        with_address = sum(1 for r in results if r['status'] == 'success' and r.get('addresses') and len(r['addresses']) > 0)
        fill_rates['addresses'] = (with_address / successful) * 100
    
    # Calculate retry statistics
    retry_stats = {
        'retried': sum(1 for r in results if r.get('retries', 0) > 0),
        'avg_retries': sum(r.get('retries', 0) for r in results) / total_websites if total_websites > 0 else 0,
        'max_retries': max((r.get('retries', 0) for r in results), default=0)
    }
    
    return {
        'total_websites': total_websites,
        'successful_scrapes': successful,
        'coverage_percentage': coverage,
        'fill_rates': fill_rates,
        'retry_stats': retry_stats
    }

if __name__ == "__main__":
    # Parameters defined in the file
    params = {
        'input': 'data/sample-websites.csv',
        'output': 'results/scraped_company_data.csv',
        'timeout': 10,
        'workers': 30,
        'no_selenium': False,
        'test': None,
        'retries': 0,
        'debug': False,
        'chrome_binary': None,
        'force_driver_update': False
    }
    
    # Configure scraper
    use_selenium = not params['no_selenium']
    
    # Set Chrome binary path from parameters or environment
    if params['chrome_binary']:
        os.environ['CHROME_BINARY_PATH'] = params['chrome_binary']
    
    scraper = CompanyScraper(
        timeout=params['timeout'], 
        use_selenium=use_selenium,
        retry_count=params['retries'],
        debug=params['debug'],
        force_driver_update=params['force_driver_update']
    )
    
    # Test a single website if specified
    if params['test']:
        print(f"Testing scraper on {params['test']}...")
        result = scraper.scrape_website(params['test'])
        print(f"\nScraping Result:")
        for key, value in result.items():
            print(f"  {key}: {value}")
        exit(0)
    
    # Load websites
    websites = load_websites(params['input'])
    print(f"Loaded {len(websites)} websites")
    
    # Measure time
    start_time = time.time()
    
    try:
        # Perform scraping with specified number of parallel workers
        results = scraper.batch_scrape(websites, max_workers=params['workers'])
        
        # Calculate elapsed time
        elapsed_time = time.time() - start_time
        if elapsed_time > 600:  # 10 minutes
            raise TimeoutError("Scraping process exceeded the 10-minute limit.")
        
        print(f"Scraping completed in {elapsed_time:.2f} seconds")
        
        # Save results
        save_results(results, params['output'])
        
        # Analyze results
        analysis = analyze_results(results)
        print("\nScraping Analysis:")
        print(f"Total websites: {analysis['total_websites']}")
        print(f"Successful scrapes: {analysis['successful_scrapes']}")
        print(f"Coverage: {analysis['coverage_percentage']:.2f}%")
        
        print("\nFill Rates:")
        for field, rate in analysis['fill_rates'].items():
            print(f"  {field}: {rate:.2f}%")
            
        print("\nRetry Statistics:")
        print(f"  Sites with retries: {analysis['retry_stats']['retried']}")
        print(f"  Average retries per site: {analysis['retry_stats']['avg_retries']:.2f}")
        print(f"  Maximum retries: {analysis['retry_stats']['max_retries']}")
    
    except TimeoutError as e:
        print(f"Error: {e}")
        exit(1)
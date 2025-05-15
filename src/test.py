import time
import argparse
import http.client
import socket
import gc 
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from time import sleep
from urllib.parse import urljoin, urlparse

# More aggressive timeout settings
http.client.HTTPConnection.timeout = 600  # 10 minutes
socket.setdefaulttimeout(600)  # Also set socket timeout to 10 minutes

class SeleniumScroller:
    
    def __init__(self, headless=False, driver_path=r'C:\Program Files\chromedriver-win64\chromedriver.exe'):
        # Set up Chrome options
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")  # New headless mode uses less memory
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")  # Reduces memory usage
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--js-flags='--expose-gc'")  # Enable manual garbage collection
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images for even less memory
        
        # Create a custom service with increased timeout
        service = Service(
            driver_path,
            service_args=['--verbose']
        )
        
        # Initialize the Chrome driver with the specified path and keep_alive=True
        self.driver = webdriver.Chrome(
            service=service,
            options=chrome_options,
            keep_alive=True  # Add this to maintain connection
        )
        
        # Set WebDriver timeouts
        self.driver.set_page_load_timeout(300)
        self.driver.set_script_timeout(300)
        
    def safe_execute_script(self, script, max_retries=3):
        """Execute JavaScript with retry mechanism for timeouts"""
        for attempt in range(max_retries):
            try:
                return self.driver.execute_script(script)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise  # Re-raise the last exception if all retries failed
                print(f"Script execution failed (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(2)  # Wait before retrying
        
    def extract_and_clear_dom(self):
        """Extract a tags and clear unnecessary DOM elements to save memory"""
        return self.safe_execute_script("""
            // First, collect all the <a> tags we want
            const anchors = Array.from(document.querySelectorAll('a'));
            const links = anchors.map(a => {
                // Extract needed information (href, text, etc.)
                return {
                    href: a.href || '',
                    text: a.textContent || '',
                    outerHTML: a.outerHTML || ''
                };
            });
            
            // Now clean up the DOM
            // 1. Remove all images (they consume a lot of memory)
            const images = document.querySelectorAll('img');
            images.forEach(img => img.remove());
            
            // 2. Remove already processed content (for example, content well above viewport)
            // This assumes we're scrolling down and won't need earlier content
            const cleanupHeight = window.scrollY - 5000; // Keep some buffer above current position
            if (cleanupHeight > 0) {
                // Find elements that are entirely above the cleanup threshold
                const elements = document.querySelectorAll('div, section, article, aside, footer');
                elements.forEach(el => {
                    const rect = el.getBoundingClientRect();
                    // If the element is entirely above our cleanup threshold
                    if (rect.bottom + window.scrollY < cleanupHeight) {
                        // Replace with a small placeholder to maintain document structure
                        const placeholder = document.createElement('div');
                        placeholder.style.height = rect.height + 'px';
                        placeholder.style.width = rect.width + 'px';
                        if (el.parentNode) {
                            el.parentNode.replaceChild(placeholder, el);
                        }
                    }
                });
            }
            
            // 3. Clear innerHTML of hidden elements 
            const hiddenElements = document.querySelectorAll('[style*="display:none"], [style*="display: none"], [hidden]');
            hiddenElements.forEach(el => {
                el.innerHTML = '';
            });
            
            // 4. Remove event listeners (can cause memory leaks)
            const allElements = document.querySelectorAll('*');
            allElements.forEach(el => {
                el.onclick = null;
                el.onmouseover = null;
                el.onmouseout = null;
            });
            
            // Force garbage collection if possible
            if (window.gc) {
                window.gc();
            }
            
            return links;
        """)

    def scroll_to_bottom(self, url, scroll_pause_time=1.0, max_scrolls=None):
        print(f'Scrolling page: {url}')
        try:
            self.driver.get(url)
        except Exception as e:
            print(f"Failed to load page: {e}")
            return None
        
        try:
            last_height = self.safe_execute_script("return document.body.scrollHeight")
        except Exception as e:
            print(f"Error getting initial scroll height: {e}")
            return None
            
        scrolls_performed = 0
        seen_links = set()  # For deduplication
        
        while True:
            if max_scrolls and scrolls_performed >= max_scrolls:
                print(f"Reached maximum number of scrolls: {max_scrolls}")
                break
            
            try:
                # Scroll to the bottom of the page to load lazy content
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                
                # Extract links and clean DOM
                links = self.extract_and_clear_dom()
                
                # Write to file with deduplication
                if links:
                    with open('./atag.txt', 'a', encoding='utf-8') as file:
                        for link in links:
                            if link['outerHTML'] and link['outerHTML'] not in seen_links:
                                seen_links.add(link['outerHTML'])
                                file.write(f"{link['outerHTML']}\n\n")  # Double newline for separation
                
                # Force Python garbage collection
                gc.collect()
                
            except Exception as e:
                print(f"Error while processing <a> tags and cleaning DOM: {e}")
                # Continue to next scroll attempt
            
            time.sleep(scroll_pause_time)
            
            try:
                new_height = self.safe_execute_script("return document.body.scrollHeight")
            except Exception as e:
                print(f"Error while getting scroll height: {e}")
                break

            scrolls_performed += 1
            print(f"Scroll #{scrolls_performed} - Height: {new_height}")
            
            if new_height == last_height:
                print("Reached the bottom of the page")
                break
                
            last_height = new_height
        
        try:
            # Instead of returning the full page source (which could be huge),
            # just return a success indicator
            return True
        except Exception as e:
            print(f"Error while finishing scroll operation: {e}")
            return None
            
    def close(self):
        """Close the browser."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                print(f"Error closing driver: {e}")


def main():
    parser = argparse.ArgumentParser(description='Scroll a webpage and extract a tags')
    parser.add_argument('--url', type=str, default='https://ekantipur.com/entertainment', 
                        help='URL to scroll')
    parser.add_argument('--headless', action='store_true', 
                        help='Run in headless mode')
    parser.add_argument('--scroll-pause', type=float, default=7.0, 
                        help='Time to pause between scrolls (seconds)')
    parser.add_argument('--max-scrolls', type=int, default=None, 
                        help='Maximum number of scrolls to perform (None = unlimited)')
    parser.add_argument('--driver-path', type=str, 
                        default=r'C:\Program Files\chromedriver-win64\chromedriver.exe',
                        help='Path to chromedriver executable')
    
    args = parser.parse_args()
    
    # Create scroller with specified options
    scroller = SeleniumScroller(headless=args.headless, driver_path=args.driver_path)

    try:
        # Wrap in try/except to ensure browser is always closed
        result = scroller.scroll_to_bottom(args.url, 
                                         scroll_pause_time=args.scroll_pause, 
                                         max_scrolls=args.max_scrolls)
        if result:
            print("Successfully completed scrolling")
    except Exception as e:
        print(f"An error occurred during scrolling: {e}")
    finally:
        # Always close the browser properly
        scroller.close()

if __name__ == "__main__":
    main()
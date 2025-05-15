import time
import re
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

class Pagination:

    def __init__(self, headless=False, driver_path=r'C:\Program Files\chromedriver-win64\chromedriver.exe'):
        """
        Scrape all article links from a category page with traditional pagination using Selenium
        
        Args:
            base_url: The base URL of the website
            category_url: The URL of the category page to scrape
        
        Returns:
            List of unique article URLs
        """
        # Setup Chrome options
        chrome_options = Options()
        if headless:
                chrome_options.add_argument("--headless=new")  # New headless mode uses less memory
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")  # Reduces memory usage
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--js-flags='--expose-gc'")  # Enable manual garbage collection
        chrome_options.add_argument("--disable-infobars")
            # chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images for even less memory
            
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
    def extract_links(self, category_url):      
        all_links = set()
        
        try:
            # Navigate to the first page
            self.driver.get(category_url)
            print(f"Navigating to {category_url}")
            
            # Wait for page to load
            time.sleep(3)
            
            # Find total number of pages
            total_pages = 1
            try:
                pagination = self.driver.find_element(By.CLASS_NAME, "ts-pagination")
                page_links = pagination.find_elements(By.CLASS_NAME, "page-numbers")
                
                for link in page_links:
                    text = link.text.strip().replace(',', '')
                    if text.isdigit():
                        page_num = int(text)
                        if page_num > total_pages:
                            total_pages = page_num
                            
                print(f"Total pages detected: {total_pages}")
            except Exception as e:
                print(f"Error detecting pagination: {e}")
                print("Defaulting to 1 page")
                total_pages = 1
            
            # # Process first page
         
            links = self.extract_article_links()
            for link in links:
                all_links.add(link['outerHTML'])
           
            
            print(f"Page 1: Found {len(links)} article links")
            
            # Process remaining pages
            for page_num in range(2, total_pages + 1):
                try:
                    page_url = f"{category_url}/page/{page_num}/"
                    print(f"Navigating to page {page_num}/{total_pages}: {page_url}")
                    
                    # Navigate to the next page
                    self.driver.get(page_url)
                    
                    # Wait for page to load
                    time.sleep(1.5)
                    
                    # Parse the page
                   
                    links = self.extract_article_links()
                    for link in links:
                        all_links.add(link['outerHTML'])
                    print(f"Page {page_num}: Found {len(links)} article links")
                    
                except Exception as e:
                    print(f"Error processing page {page_num}: {e}")
                    continue
            
            return list(all_links)
        
        finally:
            self.driver.quit()

    def extract_article_links(self):
        """
        Extract article links from the current page
        
        Args:
            soup: BeautifulSoup object of the page
            base_url: Base URL to prepend to relative URLs
        
        Returns:
            List of article URLs
        """
    
        return self.driver.execute_script("""
                // First, collect all the <a> tags we want
                const anchors = Array.from(document.querySelectorAll('a'));
                const links = anchors.map(a => {
                    // Extract needed information (href, text, etc.)
                    return {
                        outerHTML: a.outerHTML || ''
                    };
                });
                
        
        return links""")

def save_links_to_file(links, filename):
    """
    Save the links to a text file
    
    Args:
        links: List of URLs to save
        filename: Name of the file to save to
    """
   
                
                # Write to file with deduplication
    if links:
            with open(filename, 'a', encoding='utf-8') as file:
                        for link in links:
                                file.write(f"{link}\n\n")  # Double newline for separation
                

if __name__ == "__main__":
    # Configuration
    
    CATEGORY_URL = "https://ejanakpurtoday.com/category/politics"  # Politics category
    OUTPUT_FILE = "atagfile.txt"
    
    pagination = Pagination()
    # Run the scraper
    print("Starting scraper...")
    article_links = pagination.extract_links(CATEGORY_URL)
    
    # Save to file
    save_links_to_file(article_links, OUTPUT_FILE)
    print(f"Scraping complete. Found {len(article_links)} unique article links.")
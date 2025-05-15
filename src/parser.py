import requests
import json
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import datetime
import re
import os
import asyncio
import html
import logging
from database import get_connection, return_connection, close_all_connections

logging.basicConfig(
    filename='database.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

async def create_table(conn):
   
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                CREATE SEQUENCE IF NOT EXISTS parse_id_seq START 1;
                                 
                CREATE TABLE IF NOT EXISTS url_parsed_content (
                    parseID TEXT PRIMARY KEY DEFAULT 'parse' || nextval('parse_id_seq'),
                    urlID TEXT NOT NULL UNIQUE,
                    extractionTimestamp TIMESTAMP NOT NULL,
                    title TEXT NOT NULL,
                    author TEXT NOT NULL,
                    publishedDate TEXT,
                    category TEXT,
                    keywords TEXT,
                    type TEXT,
                    articleBody Text,
                    wordCount INTEGER,
                    textLength INTEGER,
                    FOREIGN KEY (urlID) REFERENCES url_registry(urlID) ON DELETE CASCADE
                    
                   
                );
                
                
            """)
        
        await conn.commit()
        logger.info("Table ensured")
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error creating table: {e}")
        
        
async def fetch_urls(conn):
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("""
                SELECT urlID, urlPath
                FROM url_registry
                WHERE index ='CC-MAIN-2025-13'
                ORDER BY RANDOM()
                LIMIT 1000;""")
                

            urls = await cursor.fetchall()
            logger.info(f"The url is fetched:{urls}")
            return urls
          
            
            
    except Exception as e:
        logger.error(f"Error fetching urls: {e}")
        

        
                                      
    
def get_user_agent():
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

def get_article_content(soup):

    # Try a variety of selectors used across different websites
    content_selectors = [
        # Common article containers
        'article', 'div.article', 'div.post', 'div.entry', 'div.content', 'div.post-content',
        'div.entry-content', 'div.article-content', 'div.story-content', 'div.main-content',

        # News site specific containers
        'div.story-body', 'div#story', 'div#article-body', 'div.article-body',
        'div.body-content', 'div.entry-body', 'div.story', 'div.news-content',

        # Blog specific containers
        'div.blog-post', 'div.blog-entry', 'div.blog-content', 'div.post-body',

        # Main content areas
        'main', 'div.main', 'div#main', 'div#content', 'div.page-content',

        # Less specific but still useful
        'div.text', 'div.body', 'section.content', 'section.article'
    ]

    # Try each selector
    for selector in content_selectors:
        try:
            # Parse the selector
            tag, classes = selector.split('.', 1) if '.' in selector else (selector, None)
            tag = tag.split('#')[0] if '#' in tag else tag

            # Search by tag and class if both are specified
            if classes:
                elements = soup.find_all(tag, class_=classes)
                if elements:
                    # Return the largest element by text content
                    return max(elements, key=lambda e: len(e.get_text(strip=True)))

            # Search by ID if specified with #
            elif '#' in selector:
                tag, id_val = selector.split('#', 1)
                element = soup.find(tag, id=id_val) if tag else soup.find(id=id_val)
                if element:
                    return element

            # Otherwise just search by tag
            else:
                elements = soup.find_all(tag)
                if elements:
                    # For common tags like 'div', try to find the one with most paragraphs
                    if tag in ['div', 'section']:
                        # Get elements with at least one paragraph
                        elements_with_p = [e for e in elements if e.find('p')]
                        if elements_with_p:
                            return max(elements_with_p, key=lambda e: len(e.find_all('p')))
                    # For article tags, just take the first one
                    if tag == 'article':
                        return elements[0]

                    # For other tags, get the one with most content
                    return max(elements, key=lambda e: len(e.get_text(strip=True)))

        except Exception as e:
            print(f"Error with selector {selector}: {e}")
            continue

    # If we get here, we couldn't find any article content with our selectors
    # Fall back to the main body content
    if soup.body:
        return soup.body

    return None

def detect_category(soup, url):
    """Detect the category of an article using multiple methods."""
    from urllib.parse import urlparse
    import json
    import re

    # Initialize category
    category = None

     # Method 4: Look for category in meta tags
    if not category:
          meta_categories = []
          meta_tags_to_check = [
              ('property', 'article:section'),
              ('name', 'category'),
              ('name', 'article:section'),
              ('name', 'sailthru.verticals'),
              ('name', 'parsely-section'),
              ('name', 'article-section'),
              ('property', 'og:section'),
              ('name', 'section'),
              ('name', 'article:tag')
          ]

          for attr, value in meta_tags_to_check:
              # Find all matching meta tags (not just the first one)
              meta_sections = soup.find_all('meta', {attr: value})
              for meta_section in meta_sections:
                  if meta_section and meta_section.get('content'):
                      meta_categories.append(meta_section['content'].lower())

          # Use the first category found if any were found
          category = meta_categories

    if not category:
      # Method 1: Extract from JSON-LD metadata
      article_sections = []
      scripts = soup.find_all('script', {"type": "application/ld+json"})

      for script in scripts:
          try:
              data = json.loads(script.string)

              if isinstance(data, list):
                  for item in data:
                      if "articleSection" in item:
                          article_sections.append(item["articleSection"])
              elif "articleSection" in data:
                  article_sections.append(data["articleSection"])
          except (json.JSONDecodeError, TypeError):
              continue

      if article_sections:
          category = article_sections

    # Method 2: Extract from HTML elements with category classes
    if not category:
        common_category_classes = [
            ".cat_name",
              ".context",
            ".card__category",
            ".cat-tag",
            ".catline",
            ".breadcrumb-item.active",
            ".breadcrumb-item",
            ".uk-light.npdate-top.uk-margin-remove.uk-h4.uk-position-relative",
            ".menu-item.menu-item-type-taxonomy.menu-item-object-category.current-post-ancestor.current-menu-parent.current-post-parent",
            ".border-start.ps-3.ms-3",
            ".active",
            ".category-list",
            ".thecategory",
          ".btn.btn-underline.mb-4.pl-0",
            ".cat-name",
            ".no-tag-title",
            ".cat-links",

            ".sub-category",
            ".badge.badge-primary",
            ".current-post-parent",
            ".items.half-more-news.category-news-list.col-12",
            ".nav-item.active",
            ".new_category",
            ".badge.badge-light.badge-category",
            ".entry-category",
            ".current-post-ancestor.current-menu-parent.current-post-parent.menu-item-has-children",
            ".current-post-ancestor.current-menu-parent.current-post-parent",
            ".breadcrumb__menu--wrapper.uk-flex.uk-flex-wrap",
            ".cat-p.text-decoration-none",
            ".category.tag",
            ".cat_matra",
            ".single_post_category",
            ".current-menu-items"
        ]

        for category_class in common_category_classes:
            category_element = soup.select_one(category_class)
            if category_element:
                category = category_element.text.strip().lower()
                break

    # Method 3: Check URL path for category indicators
    if not category:
        path = urlparse(url).path.strip('/').split('/')
        common_categories = [ 'international','sports', 'sport', 'politics', 'video', 'entertainment', 'business',
                             'technology', 'tech', 'health', 'science', 'travel', 'food', 'lifestyle',
                             'opinion', 'education', 'culture', 'finance', 'world', 'national',
                             'local', 'weather', 'environment', 'economy', 'real-estate', 'fashion',
                             'music', 'movies', 'television', 'tv', 'books', 'art', 'celebrity', 'art-literature',
                             'editorial', 'election-updates', 'society', 'kinmel', 'nepali-brand', 'cover-story', 'news',]

        for segment in path:
            if segment.lower() in common_categories:
                category = segment.lower()
                break


    # Method 5: Look for breadcrumbs
    if not category:
        breadcrumb_indicators = ['breadcrumb', 'breadcrumbs', 'path', 'navigation', 'crumbs']

        breadcrumbs = None
        for indicator in breadcrumb_indicators:
            breadcrumbs = (
                soup.find('ul', class_=lambda x: x and (indicator in x.lower())) or
                soup.find('nav', class_=lambda x: x and (indicator in x.lower())) or
                soup.find('div', class_=lambda x: x and (indicator in x.lower())) or
                soup.find('ol', class_=lambda x: x and (indicator in x.lower()))
            )

            if breadcrumbs:
                break

        if breadcrumbs:
            list_items = breadcrumbs.find_all('li') or breadcrumbs.find_all('a')
            if list_items and len(list_items) > 1:
                # Usually the second item in breadcrumbs is the category
                category_text = list_items[1].get_text().strip().lower()
                for cat in common_categories:
                    if cat in category_text:
                        category = cat
                        break
                if not category:
                    # Clean the text to use as category
                    category_text = re.sub(r'[^a-z0-9-]', '-', category_text)
                    category_text = re.sub(r'-+', '-', category_text).strip('-')
                    if category_text:
                        category = category_text

    # Method 6: Look for category in specific div elements
    if not category:
        category_indicators = ['category', 'tag', 'topic', 'section']
        for indicator in category_indicators:
            category_div = soup.find('div', class_=lambda x: x and (indicator in x.lower()))
            if category_div:
                category_text = category_div.get_text().strip().lower()
                for cat in common_categories:
                    if cat in category_text:
                        category = cat
                        break
                if category:
                    break

    # Method 7: Look for tags that might indicate category
    if not category:
        tag_containers = ['tags', 'tag-list', 'topics', 'categories']
        for container in tag_containers:
            tags_div = soup.find('div', class_=lambda x: x and (container in x.lower()))
            if tags_div and tags_div.find_all('a'):
                for tag in tags_div.find_all('a'):
                    tag_text = tag.get_text().strip().lower()
                    for cat in common_categories:
                        if cat == tag_text:
                            category = cat
                            break
                    if category:
                        break
            if category:
                break

   # Method 8: Look for elements with data-category attribute or similar data attributes
    if not category:
        # Find any element with data-category attribute
        elements_with_data_category = soup.find_all(attrs={"data-category": True})
        for element in elements_with_data_category:
            if element.get('data-category'):
                category = element['data-category'].lower()
                break

        # If still no category, check for data-cat-slug attribute
        if not category:
            elements_with_data_cat_slug = soup.find_all(attrs={"data-cat-slug": True})
            for element in elements_with_data_cat_slug:
                if element.get('data-cat-slug'):
                    category = element['data-cat-slug'].lower()
                    break

        # Also check specific elements that commonly have these attributes
        if not category:
            category_id_elements = [
                soup.find('div', id='ga-data'),
                soup.find('div', class_='ga-data'),
                soup.find('article'),
                soup.find('main')
            ]

            for element in category_id_elements:
                if element:
                   # Try data-category attribute
                    if element.get('data-category'):
                        category = element['data-category'].lower()
                        break
                    # Try data-cat-slug attribute
                    elif element.get('data-cat-slug'):
                        category = element['data-cat-slug'].lower()
                        break
                    # Try data-section attribute
                    elif element.get('data-section'):
                        category = element['data-section'].lower()
                        break

    # Method 9: If still no category, try to extract from canonical URL
    if not category:
        canonical = soup.find('link', {'rel': 'canonical'})
        if canonical and canonical.get('href'):
            canon_path = urlparse(canonical['href']).path.strip('/').split('/')
            for segment in canon_path:
                if segment.lower() in common_categories:
                    category = segment.lower()
                    break

    # Default category if none found
    if not category:
        category = "uncategorized"

    return category

def extract_publication_date(soup, url):
    """
    Extract publication date from webpage using multiple methods

    Args:
        soup: BeautifulSoup object
        url: URL of the webpage

    Returns:
        str: Publication date if found, None otherwise
    """
    # Method 1: Extract from JSON-LD structured data (most reliable)
    json_ld_date = extract_from_json_ld(soup)
    if json_ld_date:
        return json_ld_date

    # Method 2: Extract from meta tags (comprehensive list)
    date_meta_tags = [
        ('property', 'article:published_time'),
        ('name', 'publication_date'),
        ('name', 'date'),
        ('property', 'og:published_time'),
        ('name', 'pubdate'),
        ('itemprop', 'datePublished'),
        ('name', 'publish-date'),
        ('name', 'article:published_time'),
        ('property', 'article:publishedTime'),
        ('name', 'PublishDate'),
        ('name', 'publishdate'),
        ('property', 'og:pubDate'),
        ('name', 'creation-date'),
        ('name', 'DC.date.issued'),
        ('name', 'DCSext.articleFirstPublished'),
        ('property', 'datePublished'),
        ('itemprop', 'dateCreated'),
        ('http-equiv', 'date'),
        ('name', 'sailthru.date'),
        ('property', 'og:article:published_time')
    ]

    for attr, value in date_meta_tags:
        date_tag = soup.find('meta', {attr: value})
        if date_tag and date_tag.get('content'):
            return date_tag['content']

     # Method 4: Try looking for time elements
    time_elements = soup.find_all('time')
    for time_elem in time_elements:
        # Check for datetime attribute first
        if time_elem.get('datetime'):
            return time_elem['datetime']
        # Otherwise use text content
        if time_elem.text.strip():
            return time_elem.text.strip()

    # Method 3: Extract from HTML elements with date classes
    common_date_classes = [
     '.post-time', '.published-date','.posted-date',  '.date', '.article-date', '.post-date', '.news-date',
    '.entry-date', '.publish-date', '.article-time', '.article__date', '.publishedDate',
    '.news__date', '.story__date', '.story-date', '.article_datetime', '.timeago',
    '.timestamp', '.ArticleTimestamp', '.article-timestamp', '.content-timestamp',
    '.post__date', '.post-timestamp', '.dateline', '.byline-timestamp', '.metadata__date',
    '.top-item-left', '.newstime.m-0.mt-1', '.post-date-grey', '.font-weight-bold',
    '.pub-date', '.designation.alt', '.designation', '.esndt', '.date-line', '.pubed',
    '.post__time', '.posted-on-nepali', '.text-prakashit-list', '.date__time', '.date-np',
    '.sticky-date-np', '.date-time-today', '.today_date',
    '.reporter-details', '.single-author-name pt-3 ml-3', '.pdate', '.HitW',
    '.span-ago-date', '.post-meta', '.today_date_div','.single-published-date',
      '.post_commentbox'


    ]

    for date_class in common_date_classes:
        date_element = soup.select_one(date_class)
        if date_element:
            return date_element.text.strip()

    # Method 4: Try looking for time elements
    time_elements = soup.find_all('time')
    for time_elem in time_elements:
        # Check for datetime attribute first
        if time_elem.get('datetime'):
            return time_elem['datetime']
        # Otherwise use text content
        if time_elem.text.strip():
            return time_elem.text.strip()

    # Method 5: Extract from elements with datetime attributes
    elements_with_datetime = soup.find_all(attrs={"datetime": True})
    for element in elements_with_datetime:
        return element['datetime']

    # Method 6: Look for data-* attributes related to dates
    date_data_attrs = [
        'data-date', 'data-publish-date', 'data-published', 'data-post-date',
        'data-timestamp', 'data-article-date'
    ]

    for attr in date_data_attrs:
        elements = soup.find_all(attrs={attr: True})

        if elements:
            return elements[0][attr]

    # Method 7: Extract from URL
    date_from_url = extract_date_from_url(url)
    if date_from_url:
        return date_from_url

    return None


def extract_from_json_ld(soup):
    """Extract date from JSON-LD structured data"""
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    for script in json_ld_scripts:
        try:
            data = json.loads(script.string)
            # Handle both single objects and arrays of objects
            if isinstance(data, list):
                for item in data:
                    date = get_date_from_json_object(item)
                    if date:
                        return date
            else:
                date = get_date_from_json_object(data)
                if date:
                    return date
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
    return None


def get_date_from_json_object(obj):
    """Extract date from a JSON-LD object"""
    if not isinstance(obj, dict):
        return None

    # Check for common date properties
    date_properties = [
        'datePublished',
        'dateCreated',
        'dateModified',
        'publishedDate',
        'pubDate',
        'published',
        'date'
    ]

    for prop in date_properties:
        if prop in obj:
            return obj[prop]

    # Check for nested objects
    for key, value in obj.items():
        if isinstance(value, dict):
            date = get_date_from_json_object(value)
            if date:
                return date
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    date = get_date_from_json_object(item)
                    if date:
                        return date
    return None


def extract_date_from_url(url):
    """Extract date from URL if it contains date patterns"""
    # Common date patterns in URLs: YYYY/MM/DD or YYYY-MM-DD
    date_patterns = [
        r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY/MM/DD or YYYY-MM-DD
        r'(\d{4})(\d{2})(\d{2})'               # YYYYMMDD
    ]

    for pattern in date_patterns:
        match = re.search(pattern, url)
        if match:
            year, month, day = match.groups()
            try:
                # Validate date
                date_obj = datetime.datetime(int(year), int(month), int(day))
                # Return ISO format
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                continue
    return None


def extract_metadata(url):
    """
    Extract metadata from a given URL
    """
    headers = {'User-Agent': get_user_agent()}

    try:
        # Fetch the webpage
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # Try to determine encoding
        encoding = response.encoding or 'utf-8-sig'

        # Parse the HTML
        soup = BeautifulSoup(response.content, 'lxml', from_encoding=encoding)

        # Get domain for link classification
        domain = urlparse(url).netloc

        # Detect category
        category = detect_category(soup, url)

        # Initialize the article data structure
        article_data = {
            'url': url,
            'extraction_timestamp': datetime.datetime.now().isoformat(),
            'title': soup.title.string.strip() if soup.title and soup.title.string else "No title",
            'category': category,
        }

        # Extract meta tags
        article_data['meta_tags'] = {}

        # Extract description
        meta_desc = soup.find('meta', {'name': 'description'}) or soup.find('meta', {'property': 'og:description'})
        if meta_desc and meta_desc.get('content'):
            article_data['meta_tags']['description'] = meta_desc['content']
        else:
            article_data['meta_tags']['description'] = "No description"

        # Extract keywords
        meta_keywords = soup.find('meta', {'name': 'keywords'})
        if meta_keywords and meta_keywords.get('content'):
            article_data['meta_tags']['keywords'] = meta_keywords['content']

        # Extract author - try multiple methods
        author = None
        # Method 1: meta tag
        meta_author = soup.find('meta', {'name': 'author'}) or soup.find('meta', {'property': 'article:author'})
        if meta_author and meta_author.get('content'):
            author = meta_author['content']

        # Method 2: byline or author class
        if not author:
            author_elements = (
                soup.find('div', class_=lambda x: x and 'byline' in str(x).lower()) or
                soup.find('div', class_=lambda x: x and 'author' in str(x).lower()) or
                soup.find('div', class_=lambda x: x and 'writer' in str(x).lower()) or
                soup.find('span', class_=lambda x: x and 'byline' in str(x).lower()) or
                soup.find('span', class_=lambda x: x and 'author' in str(x).lower()) or
                soup.find('span', class_=lambda x: x and 'writer' in str(x).lower()) or
                soup.find('span', class_=lambda x: x and 'reporter' in str(x).lower())

            )

            if not author_elements:
              # Try to find anchor tags with author href
              author_links = soup.find_all('a', href=lambda x: x and '/author/' in x)

              # If found, check for spans inside them
              for link in author_links:
                  span = link.find('span')
                  if span:
                      author_elements = span
                      break

            if author_elements:
                author = author_elements.get_text(strip=True)

        if author:
            article_data['meta_tags']['author'] = author

        # Extract publication date - try multiple methods
        pub_date = extract_publication_date(soup, url)
        
        article_data['meta_tags']['publication_date'] = pub_date if pub_date is None else html.unescape(pub_date)

        
        
    

      
       
        # Try to find the article content
        article_body = get_article_content(soup)


        if article_body:

            paragraphs = []
            for p in soup.find_all('p'):
                # Check if this paragraph is inside another paragraph
                if not any(parent.name == 'p' for parent in p.parents) and len(p.get_text(strip =True).split())>10:
                    paragraphs.append(p)

            # Extract text from these non-nested paragraphs
            text = ' '.join(p.get_text(strip=True) for p in paragraphs)
            article_data['text'] = text

        else:
            # If no article container found, use the body
            if soup.body:
                for script in soup.body.find_all(['script', 'style', 'nav', 'header', 'footer']):
                    script.decompose()
                article_data['text'] = soup.body.get_text(separator=' ', strip=True)
                article_data['text'] = re.sub(r'\s+', ' ', article_data['text']).strip()
            else:
                article_data['text'] = "No content found"

        if not article_data['text'] or article_data['text'] == "":
            meta_description = soup.find('meta', attrs={'name': 'description'}) or soup.find('meta', attrs={'property': 'og:description'})
            if meta_description and meta_description.get('content'):
                article_data['text'] = meta_description.get('content')
            else:
                article_data['text'] = "No content found"

        # Count links and images
        article_data['statistics'] = {
    
            'text_length': len(article_data['text']),
            'word_count': len(article_data['text'].split())
        }

        return article_data

    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
        return {"error": str(e), "url": url}
    except Exception as e:
        print(f"Error processing URL: {e}")
        return {"error": str(e), "url": url}
    
 
async def update_status(conn, url_id, status): 
    try:
        async with conn.cursor() as cursor:
            
            await cursor.execute(
            """
                    UPDATE url_registry
                    SET status = %s
                    WHERE urlID = %s
            """, (status, url_id))
        
        await conn.commit()
        logger.info(f"Updated URL ID {url_id} status to {status}")
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error updating status for URL ID {url_id}: {e}") 
 
async def store_url_content(conn):
    url_paths = await fetch_urls(conn)
    
    if not url_paths:
        logger.info("Urls not fetched for the url_registry table")
        return
    
    try:
        async with conn.cursor() as cursor:
            for url_path in url_paths:
                url_id = url_path[0]
                url = url_path[1]
                
                try:
                    data = extract_metadata(url)
                    
                    # Check if extract_metadata returned an error
                    if isinstance(data, dict) and "error" in data:
                        error_msg = data["error"]
                        print(f"HTTP Error detected: {error_msg}")
                        
                        # Check for HTTP error codes
                        if any(code in error_msg for code in ["204", "404", "410", "403", "451"]) or "NameResolutionError" in error_msg or "ConnectionError" in error_msg:
                            await update_status(conn, url_id, "fail")
                            continue  # Skip to the next URL
                                            
                    # Proceed with normal insertion if no HTTP error
                    await cursor.execute("""
                        INSERT INTO url_parsed_content (urlID, extractionTimestamp, title, author, publishedDate, category, keywords, articleBody, wordCount, textLength)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (url_id, data.get('extraction_timestamp', datetime.datetime.now().isoformat()), data.get('title', 'N/A'), data.get('meta_tags', {}).get('author', 'N/A'), data.get('meta_tags', {}).get('publication_date', 'N/A'), data.get('category', 'N/A'), data.get('meta_tags', {}).get('keywords', 'N/A'), data.get('text', 'N/A'), data.get('statistics', {}).get('word_count', 0), data.get('statistics', {}).get('text_length', 0))    
                    )
                    
                    await conn.commit()
                    logger.info(f"Processed Domain:{url_id}")
                    # Update status to success
                    await update_status(conn, url_id, "success")
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"Error:{error_msg}")
                    
                    # Handle any other errors as before
                    if any(code in error_msg for code in ["204", "404", "410", "403", "451"]):
                         await update_status(conn, url_id, "fail")
                    else:
                        await update_status(conn, url_id, "success")
        
        logger.info("Finished processing all URLs")
        
    except Exception as e:
        await conn.rollback()
        logger.error(f"Error processing url_paths {url_paths}: {e}")

async def main():
    conn = await get_connection()
    
    try:
        # Create table structure
        await create_table(conn)
        await store_url_content(conn)
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
    finally:
        if conn:
            await return_connection(conn)
            logger.info("Database connection closed")
   
    
if __name__ == "__main__":
    asyncio.run(main())
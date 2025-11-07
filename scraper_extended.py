"""
Extended Multi-Site Scraper - Aims for 200+ products
Collects from: Tdiscount, Darty, Fnac, Mytek with pagination and better discovery
"""

import csv
import time
import logging
import re
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}


def clean_price(price_str: str) -> Optional[float]:
    """Extract numeric price"""
    if not price_str:
        return None
    cleaned = re.sub(r'[^\d.,]', '', str(price_str))
    cleaned = cleaned.replace(',', '.')
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def clean_rating(rating_str: str) -> Optional[float]:
    """Extract numeric rating"""
    if not rating_str:
        return None
    match = re.search(r'(\d+\.?\d*)', str(rating_str))
    if match:
        try:
            val = float(match.group(1))
            return val if 0 <= val <= 5 else None
        except ValueError:
            return None
    return None


def clean_review_count(count_str: str) -> Optional[int]:
    """Extract numeric review count"""
    if not count_str:
        return None
    match = re.search(r'(\d+)', str(count_str))
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def scrape_site_extended(base_domain: str, site_name: str, categories: List[Tuple[str, str]], pages_per_cat: int = 3):
    """Generic scraper with pagination support"""
    logger.info("\n" + "="*70)
    logger.info(f"SCRAPING {site_name.upper()}")
    logger.info("="*70)
    
    products = []
    session = requests.Session()
    session.headers.update(HEADERS)
    
    for cat_url, cat_name in categories:
        logger.info(f"\n→ {cat_name}")
        
        # Try multiple pages
        for page in range(1, pages_per_cat + 1):
            page_url = f"{cat_url}?page={page}" if page > 1 else cat_url
            
            try:
                response = session.get(page_url, timeout=10, headers=HEADERS)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find all divs with prices
                all_divs = soup.find_all('div')
                logger.info(f"  Page {page}: Scanning {len(all_divs)} divs...")
                
                page_products = 0
                for div in all_divs:
                    try:
                        text = div.get_text()
                        
                        # Must have price
                        price_match = re.search(r'(\d+[\d\s,\.]*)\s*(TND|DT|د\.ت)', text)
                        if not price_match:
                            continue
                        
                        # Reasonable text length
                        if len(text) < 20 or len(text) > 1500:
                            continue
                        
                        # Must have link
                        link_tag = div.find('a', href=True)
                        if not link_tag:
                            continue
                        
                        link = urljoin(base_domain, link_tag['href'])
                        
                        # Skip non-product links
                        if any(x in link.lower() for x in ['#', 'javascript', 'store/', 'add-to-cart', 'categorie']):
                            continue
                        
                        # Get name
                        name_elem = div.find(['h3', 'h2', 'strong'])
                        if not name_elem:
                            name_elem = link_tag.find(['span', 'h3', 'h2'])
                        if not name_elem:
                            name_elem = link_tag
                        
                        name = name_elem.text.strip() if name_elem else ''
                        name = re.sub(r'\s+', ' ', name)
                        
                        # Validate name
                        if len(name) < 5 or len(name) > 250:
                            continue
                        
                        if name.lower() in ['ajouter au panier', 'vendu par :', 'compare', 'liste de souhaits', 'demander un devis', 'effacer les filtres', 'bon plan', 'newsletter', 'satisfait ou remboursé', 'gratuite en 48h à partir de 300dt', 'jeux de construction', 'loisirs créatifs']:
                            continue
                        
                        if re.match(r'^-?\d+%$', name):
                            continue
                        
                        # Get price
                        price = clean_price(price_match.group(1))
                        if not price or price > 100000 or price < 1:
                            continue
                        
                        # Get rating
                        rating_elem = div.find(['span', 'div'], class_=re.compile('rating|note|star|avis', re.I))
                        rating = clean_rating(rating_elem.text if rating_elem else '')
                        
                        # Get review count
                        review_match = re.search(r'(\d+)\s*(avis|reviews|commentaires)', text, re.I)
                        reviews = clean_review_count(review_match.group(1)) if review_match else None
                        
                        product = {
                            'Nom': name,
                            'Prix': price,
                            'Catégorie': cat_name,
                            'Note': rating,
                            'Nb_avis': reviews,
                            'En_stock': 'Oui',
                            'Lien': link,
                            'Site': site_name
                        }
                        
                        # Avoid duplicates
                        if not any(p['Lien'] == link for p in products):
                            products.append(product)
                            page_products += 1
                    
                    except Exception as e:
                        continue
                
                logger.info(f"    Found {page_products} products on page {page}")
                time.sleep(1.5)
                
                # Stop if no products found on this page
                if page_products == 0 and page > 1:
                    break
            
            except Exception as e:
                logger.warning(f"Error on page {page}: {e}")
                continue
    
    logger.info(f"\n✓ {site_name}: {len(products)} products")
    return products


def scrape_tdiscount():
    """Scrape Tdiscount.tn with pagination"""
    categories = [
    ('https://www.tdiscount.tn/informatique', 'Informatique'),
    ('https://www.tdiscount.tn/telephonie', 'Téléphonie'),
    ('https://www.tdiscount.tn/beaute', 'Beauté'),
    ('https://www.tdiscount.tn/electromenager', 'Électroménager'),
    ('https://tdiscount.tn/categorie-produit/electromenager/gros-electromenager/', 'Électroménager'),
    ('https://tdiscount.tn/categorie-produit/electromenager/machine-a-cafe/', 'Électroménager'),
]

    return scrape_site_extended('https://www.tdiscount.tn', 'Tdiscount', categories, pages_per_cat=6)


def scrape_darty():
    """Scrape Darty.tn with pagination"""
    categories = [
        
    ('https://darty.tn/247-hard-produits-maitres', 'Informatique'),
    ('https://darty.tn/247-hard-produits-maitres', 'Informatique'),
    ('https://darty.tn/248-ordinateurs-portables', 'Informatique'),
    ('https://darty.tn/422-ordinateurs-de-bureau', 'Informatique'),
    ('https://darty.tn/45-telephonie-mobilite', 'Téléphonie'),
    ('https://darty.tn/291-telephonie-mobile', 'Téléphonie'),
    ('https://darty.tn/291-telephonie-mobile?page=2', 'Téléphonie'),
    ('https://darty.tn/173-beaute-sante-et-hygiene', 'Beauté'),
    ('https://darty.tn/179-seche-cheveux', 'Beauté'),
    ('https://darty.tn/181-hygiene-dentaire', 'Beauté'),
    ('https://darty.tn/190-epilation', 'Beauté'),
    ('https://darty.tn/10-10-gros-electromenager', 'Électroménager'),
    ('https://darty.tn/13-petit-electromenager', 'Électroménager'),
    ('https://darty.tn/14-petit-dejeuner', 'Électroménager'),
    ('https://darty.tn/294-traitement-sol', 'Électroménager'),
    ('https://darty.tn/10-10-gros-electromenager', 'Électroménager'),
    ('https://darty.tn/141-lavage', 'Électroménager'),
    ('https://darty.tn/21-televiseurs-tv-led', 'Électroménager'),
    ('https://darty.tn/123-smart-tv-et-televiseur', 'Électroménager'),
    ('https://darty.tn/141-lavage', 'Électroménager')
]
     
       
    
    return scrape_site_extended('https://darty.tn', 'Darty', categories, pages_per_cat=6)


def scrape_fnac():
    """Scrape Fnac.tn with pagination"""
    categories = [
        ('https://www.fnac.tn/informatique', 'Informatique'),
        ('https://fnac.tn/49-informatique-pc-tablettes', 'Informatique'),
        ('https://fnac.tn/56-pc-portables-et-laptops','Informatique'),
        ('https://www.fnac.tn/telephonie', 'Téléphonie'),
        ('https://www.fnac.tn/beaute', 'Beauté'),
        ('https://www.fnac.tn/electromenager', 'Électroménager'),
        ('https://fnac.tn/211-son-casques-enceintes', 'Informatique'),
        ('https://fnac.tn/477-radio', 'Informatique'),
        ('https://fnac.tn/106-smartphones-objets-connectes', 'Téléphonie'),
    ]
    return scrape_site_extended('https://www.fnac.tn', 'Fnac', categories, pages_per_cat=5)


def scrape_fatale():
    """Scrape Fatale.tn"""
    categories = [
        ('https://www.fatales.tn/417-maquillage', 'Beauté'),
        ('https://www.fatales.tn/426-soins-visage', 'Beauté'),
        ('https://www.fatales.tn/428-fragrance', 'Beauté'),
        ('https://www.fatales.tn/417-maquillage?page=2', 'Beauté'),
        ('https://www.fatales.tn/426-soins-visage?page=3', 'Beauté'),
        ('https://www.fatales.tn/426-soins-visage?page=4', 'Beauté'),
        ('https://www.fatales.tn/426-soins-visage?page=5', 'Beauté'),
    ]
    return scrape_site_extended('https://www.fatales.tn', 'Fatale', categories, pages_per_cat=5)


def scrape_mytek():
    """Scrape Mytek.tn using Selenium for JavaScript-rendered content"""
    logger.info("\n" + "="*70)
    logger.info("SCRAPING MYTEK.TN (SELENIUM)")
    logger.info("="*70)
    
    products = []
    
    # Mytek categories
    categories = [
        ('https://www.mytek.tn/informatique', 'Informatique'),
        ('https://www.mytek.tn/telephonie-tunisie', 'Téléphonie'),
        ('https://www.mytek.tn/electromenager', 'Électroménager'),
        # Beauté static URLs
        ('https://www.mytek.tn/mode-beaute-sante/bijouterie.html', 'Beauté'),
        ('https://www.mytek.tn/mode-beaute-sante/parfums.html', 'Beauté'),
        ('https://www.mytek.tn/mode-beaute-sante/epilation.html', 'Beauté'),
        ('https://www.mytek.tn/mode-beaute-sante/hygiene-soin-beaute.html', 'Beauté'),
        ('https://www.mytek.tn/mode-beaute-sante/soins-femme.html', 'Beauté'),
        ('https://www.mytek.tn/mode-beaute-sante/soins-homme.html', 'Beauté'),
    ]
    
    # Setup Selenium
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('user-agent=' + HEADERS['User-Agent'])
    
    driver = None
    try:
        # Initialize Selenium driver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(20)
        
        for cat_url, cat_name in categories:
            logger.info(f"\n→ {cat_name}: {cat_url}")
            
            try:
                # Load page with Selenium
                driver.get(cat_url)
                time.sleep(3)  # Wait for JS to render
                
                # Get rendered HTML
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Find all divs with prices
                all_divs = soup.find_all('div')
                logger.info(f"  Scanning {len(all_divs)} divs...")
                
                for div in all_divs:
                    try:
                        text = div.get_text()
                        
                        # Must have price
                        price_match = re.search(r'(\d+[\d\s,\.]*)\s*(TND|DT|د\.ت)', text)
                        if not price_match:
                            continue
                        
                        # Reasonable text length
                        if len(text) < 20 or len(text) > 1500:
                            continue
                        
                        # Must have link
                        link_tag = div.find('a', href=True)
                        if not link_tag:
                            continue
                        
                        link = urljoin('https://www.mytek.tn', link_tag['href'])
                        
                        # Skip non-product links
                        if any(x in link.lower() for x in ['#', 'javascript', 'categorie']):
                            continue
                        
                        # Get name
                        name_elem = div.find(['h3', 'h2', 'strong'])
                        if not name_elem:
                            name_elem = link_tag.find(['span', 'h3', 'h2'])
                        if not name_elem:
                            name_elem = link_tag
                        
                        name = name_elem.text.strip() if name_elem else ''
                        name = re.sub(r'\s+', ' ', name)
                        
                        # Validate name
                        if len(name) < 5 or len(name) > 250:
                            continue
                        
                        if name.lower() in ['ajouter au panier', 'vendu par :', 'compare', 'liste de souhaits', 'demander un devis', 'effacer les filtres', 'bon plan', 'newsletter', 'satisfait ou remboursé', 'gratuite en 48h à partir de 300dt', 'jeux de construction', 'loisirs créatifs']:
                            continue
                        
                        if re.match(r'^-?\d+%$', name):
                            continue
                        
                        # Get price
                        price = clean_price(price_match.group(1))
                        if not price or price > 100000 or price < 1:
                            continue
                        
                        # Get rating
                        rating_elem = div.find(['span', 'div'], class_=re.compile('rating|note|star|avis', re.I))
                        rating = clean_rating(rating_elem.text if rating_elem else '')
                        
                        # Get review count
                        review_match = re.search(r'(\d+)\s*(avis|reviews|commentaires)', text, re.I)
                        reviews = clean_review_count(review_match.group(1)) if review_match else None
                        
                        product = {
                            'Nom': name,
                            'Prix': price,
                            'Catégorie': cat_name,
                            'Note': rating,
                            'Nb_avis': reviews,
                            'En_stock': 'Oui',
                            'Lien': link,
                            'Site': 'Mytek'
                        }
                        
                        # Avoid duplicates
                        if not any(p['Lien'] == link for p in products):
                            products.append(product)
                            logger.info(f"  ✓ {name[:45]} - {price} DT")
                    
                    except Exception as e:
                        continue
                
                time.sleep(1.5)
            
            except Exception as e:
                logger.warning(f"Error: {e}")
    
    finally:
        if driver:
            driver.quit()
    
    logger.info(f"\n✓ Mytek: {len(products)} products")
    return products


def export_csv(all_products: List[Dict]):
    """Export to professional CSV"""
    fieldnames = ['Nom', 'Prix', 'Catégorie', 'Note', 'Nb_avis', 'En_stock', 'Lien', 'Site']
    
    filename = 'produits_complet.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for product in all_products:
            row = {
                'Nom': product.get('Nom', ''),
                'Prix': product.get('Prix', ''),
                'Catégorie': product.get('Catégorie', ''),
                'Note': product.get('Note', ''),
                'Nb_avis': product.get('Nb_avis', ''),
                'En_stock': product.get('En_stock', ''),
                'Lien': product.get('Lien', ''),
                'Site': product.get('Site', '')
            }
            writer.writerow(row)
    
    logger.info(f"\n✓ Exported {len(all_products)} products to {filename}")


def main():
    logger.info("="*70)
    logger.info("EXTENDED MULTI-SITE PRODUCT SCRAPER - TARGET: 250+ PRODUCTS")
    logger.info("Sites: Tdiscount, Darty, Fnac, Fatale, Mytek")
    logger.info("="*70)
    
    all_products = []
    
    # Scrape all sites
    try:
        tdiscount_products = scrape_tdiscount()
        all_products.extend(tdiscount_products)
    except Exception as e:
        logger.error(f"Tdiscount error: {e}")
    
    try:
        darty_products = scrape_darty()
        all_products.extend(darty_products)
    except Exception as e:
        logger.error(f"Darty error: {e}")
    
    try:
        fnac_products = scrape_fnac()
        all_products.extend(fnac_products)
    except Exception as e:
        logger.error(f"Fnac error: {e}")
    
    try:
        fatale_products = scrape_fatale()
        all_products.extend(fatale_products)
    except Exception as e:
        logger.error(f"Fatale error: {e}")
    
    try:
        mytek_products = scrape_mytek()
        all_products.extend(mytek_products)
    except Exception as e:
        logger.error(f"Mytek error: {e}")
    
    # Deduplicate by URL
    unique = []
    seen = set()
    for p in all_products:
        if p['Lien'] not in seen:
            unique.append(p)
            seen.add(p['Lien'])
    
    logger.info(f"\n{'='*70}")
    logger.info(f"FINAL SUMMARY")
    logger.info(f"{'='*70}")
    logger.info(f"Total products collected: {len(all_products)}")
    logger.info(f"Total unique products: {len(unique)}")
    
    # Export
    export_csv(unique)
    logger.info(f"{'='*70}")
    logger.info("✓ Scraping complete!")


if __name__ == '__main__':
    main()

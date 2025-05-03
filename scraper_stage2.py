"""
Stage 2: Extract links from scraped HTML files.
"""
import argparse
import concurrent.futures
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

import config
from utils import logger, save_to_file, get_progress_bar


class LinkExtractor:
    """
    Extract links from HTML files scraped in Stage 1.
    """
    
    def __init__(self, input_dir: Path = config.DATA_DIR, output_file: Path = config.LINKS_CSV):
        """
        Initialize the link extractor.
        
        Args:
            input_dir: Directory containing HTML files
            output_file: Output CSV file for links
        """
        self.input_dir = Path(input_dir)
        self.output_file = Path(output_file)
        
        # Ensure output directory exists
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
    def extract_link_from_file(self, file_path: Path) -> Optional[str]:
        """
        Extract link from a single HTML file.
        
        Args:
            file_path: Path to HTML file
            
        Returns:
            Optional[str]: Extracted link or None if not found
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                
            soup = BeautifulSoup(content, "html.parser")
            
            # Try different selectors to find links
            link_tag = soup.find("a", href=True)
            
            if not link_tag:
                # Try alternative selectors
                link_tag = soup.select_one(".gallery-card a")
                
            if not link_tag:
                logger.warning(f"No link found in {file_path}")
                return None
                
            link = link_tag["href"]
            
            # Ensure link is absolute
            if link.startswith("//"):
                link = "https:" + link
            elif link.startswith("/"):
                link = "https://craigslist.org" + link
                
            return link
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            return None
    
    def extract_all_links(self) -> List[str]:
        """
        Extract links from all HTML files in the input directory.
        
        Returns:
            List[str]: List of extracted links
        """
        links = []
        html_files = list(self.input_dir.glob("*.html"))
        
        if not html_files:
            logger.error(f"No HTML files found in {self.input_dir}")
            return []
            
        logger.info(f"Found {len(html_files)} HTML files to process")
        
        for i, file_path in enumerate(html_files):
            link = self.extract_link_from_file(file_path)
            if link:
                links.append(link)
                
            # Show progress
            if (i + 1) % 10 == 0 or i == len(html_files) - 1:
                progress = get_progress_bar(i + 1, len(html_files))
                logger.info(f"Progress: {progress}")
                
        return links
    
    def extract_links_parallel(self, num_workers: int = 4) -> List[str]:
        """
        Extract links using parallel processing for better performance.
        
        Args:
            num_workers: Number of parallel workers
            
        Returns:
            List[str]: List of extracted links
        """
        html_files = list(self.input_dir.glob("*.html"))
        
        if not html_files:
            logger.error(f"No HTML files found in {self.input_dir}")
            return []
            
        logger.info(f"Found {len(html_files)} HTML files to process")
        links = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Map the function to all files
            future_to_file = {
                executor.submit(self.extract_link_from_file, file_path): file_path 
                for file_path in html_files
            }
            
            # Process results as they complete
            with tqdm(total=len(html_files), desc="Extracting links") as pbar:
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        link = future.result()
                        if link:
                            links.append(link)
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Error processing {file_path}: {e}")
                        pbar.update(1)
        
        return links
    
    def save_links_to_csv(self, links: List[str]) -> bool:
        """
        Save extracted links to CSV file.
        
        Args:
            links: List of links to save
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            df = pd.DataFrame({"link": links})
            
            # Add metadata columns
            df["scraped"] = False
            df["processed"] = False
            
            # Save to CSV
            df.to_csv(self.output_file, index=False)
            logger.info(f"Saved {len(links)} links to {self.output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving links to CSV: {e}")
            return False
    
    def run(self, parallel: bool = True, num_workers: int = 4) -> bool:
        """
        Run the link extraction process.
        
        Args:
            parallel: Whether to use parallel processing
            num_workers: Number of parallel workers
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Starting link extraction")
        
        if parallel:
            links = self.extract_links_parallel(num_workers)
        else:
            links = self.extract_all_links()
            
        if not links:
            logger.error("No links extracted")
            return False
            
        logger.info(f"Extracted {len(links)} links")
        return self.save_links_to_csv(links)


def main():
    """
    Main function to run the link extractor from command line.
    """
    parser = argparse.ArgumentParser(description="Craigslist Link Extractor - Stage 2")
    parser.add_argument("--input", default=str(config.DATA_DIR), help="Input directory with HTML files")
    parser.add_argument("--output", default=str(config.LINKS_CSV), help="Output CSV file for links")
    parser.add_argument("--no-parallel", action="store_true", help="Disable parallel processing")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    
    args = parser.parse_args()
    
    extractor = LinkExtractor(
        input_dir=Path(args.input),
        output_file=Path(args.output)
    )
    
    success = extractor.run(
        parallel=not args.no_parallel,
        num_workers=args.workers
    )
    
    if success:
        logger.info("Link extraction completed successfully")
    else:
        logger.error("Link extraction failed")


if __name__ == "__main__":
    main()

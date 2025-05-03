"""
Main script to run the entire Craigslist scraper pipeline.
"""
import argparse
import os
import sys
import time
from pathlib import Path

import config
from utils import logger
from scraper_stage1 import ListingScraper
from scraper_stage2 import LinkExtractor
from scraper_stage3 import DetailScraper
from scraper_stage4 import DataExtractor
from scraper_stage5 import DataFilter


def run_pipeline(
    base_url: str,
    stages: list = [1, 2, 3, 4, 5],
    parallel: bool = True,
    max_pages: int = None,
    workers: dict = None
):
    """
    Run the entire scraper pipeline.

    Args:
        base_url: Base URL for Craigslist search
        stages: List of stages to run
        parallel: Whether to use parallel processing
        max_pages: Maximum number of pages to scrape
        workers: Dictionary of worker counts for each stage
    """
    start_time = time.time()

    if workers is None:
        workers = {
            1: 3,  # Stage 1: Listing scraper
            2: 4,  # Stage 2: Link extractor
            3: 2,  # Stage 3: Detail scraper
            4: 4,  # Stage 4: Data extractor
            5: 1   # Stage 5: Data filter (single-threaded)
        }

    # Sort stages to ensure they run in order
    stages = sorted([s for s in stages if 1 <= s <= 5])

    # Track which stages have been completed
    completed_stages = []

    # Function to run a specific stage
    def run_stage(stage_num):
        try:
            if stage_num == 1:
                # Stage 1: Scrape listings
                logger.info("=== STAGE 1: SCRAPING LISTINGS ===")
                logger.info("Press Ctrl+C to skip to the next stage")
                scraper = ListingScraper(
                    base_url=base_url,
                    output_dir=config.DATA_DIR,
                    max_pages=max_pages
                )

                if parallel:
                    scraper.scrape_with_parallel_processing(num_workers=workers[1])
                else:
                    scraper.scrape_all_pages()

            elif stage_num == 2:
                # Stage 2: Extract links
                logger.info("=== STAGE 2: EXTRACTING LINKS ===")
                logger.info("Press Ctrl+C to skip to the next stage")
                extractor = LinkExtractor(
                    input_dir=config.DATA_DIR,
                    output_file=config.LINKS_CSV
                )

                extractor.run(parallel=parallel, num_workers=workers[2])

            elif stage_num == 3:
                # Stage 3: Scrape details
                logger.info("=== STAGE 3: SCRAPING DETAILS ===")
                logger.info("Press Ctrl+C to skip to the next stage")
                detail_scraper = DetailScraper(
                    input_file=config.LINKS_CSV,
                    output_dir=config.MAIN_DATA_DIR,
                    resume=True
                )

                if parallel:
                    detail_scraper.scrape_with_parallel_processing(num_workers=workers[3])
                else:
                    detail_scraper.scrape_all_listings()

            elif stage_num == 4:
                # Stage 4: Extract data
                logger.info("=== STAGE 4: EXTRACTING DATA ===")
                logger.info("Press Ctrl+C to skip to the next stage")
                data_extractor = DataExtractor(
                    input_dir=config.MAIN_DATA_DIR,
                    links_file=config.LINKS_CSV,
                    output_file=config.OUTPUT_CSV
                )

                data_extractor.run(parallel=parallel, num_workers=workers[4])

            elif stage_num == 5:
                # Stage 5: Filter data
                logger.info("=== STAGE 5: FILTERING DATA ===")
                logger.info("Press Ctrl+C to skip to the next stage")
                data_filter = DataFilter(
                    input_file=config.OUTPUT_CSV,
                    output_file=config.FILTERED_CSV,
                    phone_required=True
                )

                data_filter.run()

            # Mark stage as completed
            completed_stages.append(stage_num)
            return True

        except KeyboardInterrupt:
            logger.info(f"\n\nStage {stage_num} interrupted by user. Skipping to next stage...")
            return False
        except Exception as e:
            logger.error(f"Error in stage {stage_num}: {e}")
            return False

    # Run each stage in order
    for stage in stages:
        if stage not in completed_stages:
            success = run_stage(stage)
            if success:
                logger.info(f"Stage {stage} completed successfully")
            else:
                logger.warning(f"Stage {stage} did not complete successfully")

    # Calculate total time
    end_time = time.time()
    total_time = end_time - start_time
    hours, remainder = divmod(total_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    logger.info(f"Pipeline completed in {int(hours)}h {int(minutes)}m {int(seconds)}s")

    # Show output file locations
    if 5 in stages:
        logger.info(f"Final filtered data saved to: {config.FILTERED_CSV}")
    elif 4 in stages:
        logger.info(f"Extracted data saved to: {config.OUTPUT_CSV}")


def main():
    """
    Main function to run the scraper from command line.
    """
    parser = argparse.ArgumentParser(description="Craigslist Scraper Pipeline")
    parser.add_argument("url", help="Base Craigslist search URL")
    parser.add_argument("--stages", type=int, nargs="+", default=[1, 2, 3, 4, 5],
                        help="Stages to run (1-5)")
    parser.add_argument("--no-parallel", action="store_true", help="Disable parallel processing")
    parser.add_argument("--max-pages", type=int, help="Maximum number of pages to scrape")
    parser.add_argument("--workers-stage1", type=int, default=3, help="Workers for stage 1")
    parser.add_argument("--workers-stage2", type=int, default=4, help="Workers for stage 2")
    parser.add_argument("--workers-stage3", type=int, default=2, help="Workers for stage 3")
    parser.add_argument("--workers-stage4", type=int, default=4, help="Workers for stage 4")

    args = parser.parse_args()

    # Validate stages
    for stage in args.stages:
        if stage < 1 or stage > 5:
            logger.error(f"Invalid stage: {stage}. Must be between 1 and 5.")
            sys.exit(1)

    # Set up worker counts
    workers = {
        1: args.workers_stage1,
        2: args.workers_stage2,
        3: args.workers_stage3,
        4: args.workers_stage4,
        5: 1  # Stage 5 is always single-threaded
    }

    logger.info(f"Starting Craigslist scraper pipeline with URL: {args.url}")
    logger.info(f"Running stages: {args.stages}")

    run_pipeline(
        base_url=args.url,
        stages=args.stages,
        parallel=not args.no_parallel,
        max_pages=args.max_pages,
        workers=workers
    )


if __name__ == "__main__":
    main()

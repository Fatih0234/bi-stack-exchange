from .analytics_utilities import AnalyticsUtilities
import pandas as pd
import logging
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

def run_all_analytics():
    try:
        # Initialize analytics utilities
        analytics = AnalyticsUtilities()
        
        # Run all analytics functions
        analytics.calculate_user_engagement_metrics()
        analytics.calculate_post_metrics()
        analytics.calculate_tag_metrics()
        analytics.calculate_badge_metrics()
        analytics.calculate_comment_metrics()
        analytics.calculate_vote_metrics()
        
        logging.info("All analytics calculations completed successfully")
    except Exception as e:
        logging.error(f"Error during analytics calculations: {e}")
        raise

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        run_all_analytics()
        logger.info("Analytics process completed successfully")
    except Exception as e:
        logger.error(f"Error during analytics process: {e}")
        raise 
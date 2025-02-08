import os
import configparser
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

class AnalyticsUtilities:
    def __init__(self, config_file='config.ini', env_file='.env'):
        # Load configuration from files
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        load_dotenv(env_file)

        # Retrieve database credentials
        self.user = self.config['PostgreSQL']['STACK_USER']
        self.host = self.config['PostgreSQL']['STACK_URL']
        self.port = self.config['PostgreSQL']['STACK_PORT']
        self.db_name = self.config['PostgreSQL']['STACK_NAME']
        self.password = os.getenv('STACK_PASSWORD')

        if not self.password:
            raise ValueError('Database password not found in .env file')

        # Initialize SQLAlchemy engine
        self.engine = self._create_engine()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def _create_engine(self):
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        engine = create_engine(url, echo=False)
        return engine

    def execute_query(self, query):
        """Executes a SQL query and returns the results as a Pandas DataFrame."""
        try:
            with self.engine.connect() as conn:
                # First, get a count of rows that would be returned
                count_query = f"WITH query AS ({query}) SELECT COUNT(*) FROM query"
                result_count = conn.execute(text(count_query)).scalar()
                self.log_message(f"Query will return {result_count} rows")
                
                # Execute the actual query
                result = conn.execute(text(query))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                if df.empty:
                    self.log_message("Query returned no results", level=logging.WARNING)
                else:
                    self.log_message(f"Query returned {len(df)} rows with columns: {list(df.columns)}")
                
                return df
        except Exception as e:
            self.log_message(f"Error executing query: {str(e)}", level=logging.ERROR)
            self.log_message(f"Query was: {query}", level=logging.ERROR)
            return pd.DataFrame()  # Return an empty DataFrame in case of error

    def most_popular_tags(self, limit=10):
        """Returns the most popular tags based on post count."""
        query = f"""
            WITH SplitTags AS (
                SELECT 
                    hp.post_business_key,
                    UNNEST(string_to_array(sp.tags, '><')) as tag
                FROM hub_post hp
                JOIN sat_post sp ON hp.post_hk = sp.post_hk
                WHERE sp.tags IS NOT NULL AND sp.tags != ''
            )
            SELECT 
                TRIM(BOTH '<>' FROM tag) as tag_name,
                COUNT(*) as usage_count
            FROM SplitTags
            WHERE tag != ''
            GROUP BY tag_name
            ORDER BY usage_count DESC
            LIMIT {limit}
        """
        return self.execute_query(query)

    def most_edited_posts(self, limit=10):
        """Returns the 10 most edited posts based on the number of edits."""
        query = f"""
            SELECT hp.post_business_key, COUNT(lpu.post_hk) AS edit_count
            FROM link_post_user lpu
            JOIN hub_post hp ON lpu.post_hk = hp.post_hk
            GROUP BY hp.post_business_key
            ORDER BY edit_count DESC
            LIMIT {limit}
        """
        return self.execute_query(query)

    def users_with_longest_comments(self, limit=10):
        """Returns users with the longest comments on average, including comment count and total length.
        Only includes users with at least 5 comments to ensure meaningful analytics."""
        query = f"""
            WITH UserComments AS (
                SELECT 
                    hu.user_business_key,
                    su.displayname,
                    sc.text as comment_text,
                    LENGTH(sc.text) as comment_length
                FROM sat_comment sc
                JOIN hub_comment hc ON hc.comment_hk = sc.comment_hk
                JOIN link_post_comment lpc ON lpc.comment_hk = hc.comment_hk
                JOIN hub_post hp ON hp.post_hk = lpc.post_hk
                JOIN link_post_user lpu ON lpu.post_hk = hp.post_hk
                JOIN hub_user hu ON hu.user_hk = lpu.user_hk
                JOIN sat_user su ON su.user_hk = hu.user_hk
                WHERE sc.text IS NOT NULL AND sc.text != ''
            )
            SELECT 
                displayname,
                COUNT(*) as total_comments,
                ROUND(AVG(comment_length)::numeric, 2) as avg_comment_length,
                MAX(comment_length) as longest_comment,
                MIN(comment_length) as shortest_comment,
                SUM(comment_length) as total_characters
            FROM UserComments
            GROUP BY user_business_key, displayname
            HAVING COUNT(*) >= 5  -- Changed to require at least 5 comments
            ORDER BY avg_comment_length DESC
            LIMIT {limit}
        """
        return self.execute_query(query)

    def fastest_commenters(self, limit=10):
        """Returns the users who comment most quickly after posts are created."""
        query = f"""
            WITH CommentTiming AS (
                SELECT 
                    hu.user_business_key,
                    su.displayname,
                    sp.load_date as post_date,
                    sc.load_date as comment_date,
                    EXTRACT(EPOCH FROM (sc.load_date - sp.load_date)) as response_time_seconds
                FROM sat_comment sc
                JOIN hub_comment hc ON hc.comment_hk = sc.comment_hk
                JOIN link_post_comment lpc ON lpc.comment_hk = hc.comment_hk
                JOIN hub_post hp ON hp.post_hk = lpc.post_hk
                JOIN sat_post sp ON sp.post_hk = hp.post_hk
                JOIN link_post_user lpu ON lpu.post_hk = hp.post_hk
                JOIN hub_user hu ON hu.user_hk = lpu.user_hk
                JOIN sat_user su ON su.user_hk = hu.user_hk
                WHERE sc.load_date > sp.load_date
            )
            SELECT 
                displayname,
                COUNT(*) as total_comments,
                ROUND(AVG(response_time_seconds)::numeric, 2) as avg_response_time_seconds,
                MIN(response_time_seconds) as fastest_response_seconds,
                MAX(response_time_seconds) as slowest_response_seconds
            FROM CommentTiming
            GROUP BY user_business_key, displayname
            HAVING COUNT(*) >= 1  -- Changed to require at least 1 comment
            ORDER BY avg_response_time_seconds ASC
            LIMIT {limit}
        """
        return self.execute_query(query)
    
    def active_vs_non_active_users(self):
        """Returns user activity classification based on their comment and post counts."""
        query = """
            WITH UserActivity AS (
                SELECT 
                    hu.user_business_key,
                    su.displayname,
                    COUNT(DISTINCT hp.post_hk) as post_count,
                    COUNT(DISTINCT hc.comment_hk) as comment_count
                FROM hub_user hu
                JOIN sat_user su ON hu.user_hk = su.user_hk
                LEFT JOIN link_post_user lpu ON hu.user_hk = lpu.user_hk
                LEFT JOIN hub_post hp ON lpu.post_hk = hp.post_hk
                LEFT JOIN link_post_comment lpc ON lpc.comment_hk = hu.user_hk
                LEFT JOIN hub_comment hc ON hc.comment_hk = lpc.comment_hk
                GROUP BY hu.user_business_key, su.displayname
            ),
            ActivityMetrics AS (
                SELECT 
                    AVG(post_count) as avg_posts,
                    AVG(comment_count) as avg_comments
                FROM UserActivity
            )
            SELECT 
                ua.user_business_key,
                ua.displayname,
                ua.post_count,
                ua.comment_count,
                CASE 
                    WHEN ua.post_count > am.avg_posts OR ua.comment_count > am.avg_comments THEN 'Active'
                    ELSE 'Non-Active'
                END as activity_status,
                am.avg_posts as average_posts,
                am.avg_comments as average_comments
            FROM UserActivity ua
            CROSS JOIN ActivityMetrics am
            ORDER BY (ua.post_count + ua.comment_count) DESC
        """
        return self.execute_query(query)

    def ration_comments_upvotes(self):
        """Calculates the ratio between comments and upvotes."""
        query = """
        SELECT 
            (SELECT COUNT(*) FROM hub_comment) AS comment_count,
            (SELECT SUM(sp.score) FROM sat_post sp) AS total_upvotes,
            CASE 
                WHEN (SELECT COUNT(*) FROM hub_comment) = 0 THEN 0
                ELSE (SELECT SUM(sp.score)::decimal FROM sat_post sp) / (SELECT COUNT(*) FROM hub_comment)
            END AS ratio
        """
        return self.execute_query(query)

    def ration_upvotes_edits(self):
        """Calculates the ratio between upvotes and edits of the posts."""
        query = """
        SELECT 
            (SELECT SUM(sp.score) FROM sat_post sp) AS total_upvotes,
            (SELECT COUNT(*) FROM link_post_user) AS total_edits,
            CASE 
                WHEN (SELECT COUNT(*) FROM link_post_user) = 0 THEN 0 
                ELSE (SELECT SUM(sp.score)::decimal FROM sat_post sp) / (SELECT COUNT(*) FROM link_post_user)
            END AS ratio
        """
        return self.execute_query(query)

    def views_per_question_ratio(self):
        """
        Ratio between views and question answers to understand database action.
        """
        query = """
        SELECT 
            AVG(sp.viewcount) AS average_views_per_question
        FROM hub_post hp
        JOIN sat_post sp ON hp.post_hk = sp.post_hk
        WHERE sp.answercount > 0
        """
        return self.execute_query(query)
    
    def inactive_tag_ratio(self):
        """
        New KPI: inactive tags ratio
        """
        query = """
        WITH TagActivity AS (
            SELECT 
                ht.tag_business_key,
                CASE 
                    WHEN COUNT(lpt.post_hk) > 0 THEN 'Active'
                    ELSE 'Inactive'
                END AS tag_status
            FROM hub_tag ht
            LEFT JOIN link_post_tag lpt ON ht.tag_hk = lpt.tag_hk
            GROUP BY ht.tag_business_key
        )
        SELECT 
            (SELECT COUNT(*) FROM TagActivity WHERE tag_status = 'Inactive') AS inactive_tag_count,
            (SELECT COUNT(*) FROM TagActivity) AS total_tag_count,
            (SELECT COUNT(*)::decimal / (SELECT COUNT(*) FROM TagActivity) 
             FROM TagActivity WHERE tag_status = 'Inactive') AS ratio
        """
        return self.execute_query(query)
    
    def avg_view_to_answer_ratio(self):
        """
        New KPI: average view-to-answer ratio
        """
        query = """
        SELECT 
            AVG(sp.viewcount::decimal / NULLIF(sp.answercount, 0)) AS average
        FROM sat_post sp
        """
        return self.execute_query(query)

    def check_data_relationships(self):
        """Checks the counts and relationships between tables."""
        query = """
        SELECT 
            (SELECT COUNT(*) FROM hub_user) as user_count,
            (SELECT COUNT(*) FROM hub_comment) as comment_count,
            (SELECT COUNT(*) FROM hub_post) as post_count,
            (SELECT COUNT(*) FROM link_post_comment) as post_comment_links,
            (SELECT COUNT(*) FROM sat_comment WHERE text IS NOT NULL AND text != '') as comments_with_text,
            (SELECT COUNT(DISTINCT comment_hk) FROM link_post_comment) as users_with_comments,
            (SELECT COUNT(*) FROM link_post_user) as post_user_links,
            (SELECT COUNT(*) FROM sat_comment sc 
             JOIN hub_comment hc ON hc.comment_hk = sc.comment_hk
             JOIN link_post_comment lpc ON lpc.comment_hk = hc.comment_hk) as actual_comments_with_links
        """
        return self.execute_query(query)

    def tag_engagement_analysis(self, limit=10):
        """Analyzes tag engagement by looking at comment counts and response times."""
        query = """
            WITH TaggedPosts AS (
                SELECT 
                    hp.post_business_key,
                    UNNEST(string_to_array(sp.tags, '><')) as tag
                FROM hub_post hp
                JOIN sat_post sp ON hp.post_hk = sp.post_hk
                WHERE sp.tags IS NOT NULL AND sp.tags != ''
            ),
            TagStats AS (
                SELECT 
                    TRIM(BOTH '<>' FROM tag) as tag_name,
                    COUNT(DISTINCT tp.post_business_key) as post_count,
                    COUNT(DISTINCT lpc.comment_hk) as comment_count,
                    ROUND(AVG(EXTRACT(EPOCH FROM (sc.load_date - sp.load_date)))::numeric, 2) as avg_response_time_seconds
                FROM TaggedPosts tp
                JOIN hub_post hp ON hp.post_business_key = tp.post_business_key
                JOIN sat_post sp ON sp.post_hk = hp.post_hk
                LEFT JOIN link_post_comment lpc ON lpc.post_hk = hp.post_hk
                LEFT JOIN hub_comment hc ON hc.comment_hk = lpc.comment_hk
                LEFT JOIN sat_comment sc ON sc.comment_hk = hc.comment_hk
                WHERE tag != ''
                GROUP BY tag_name
                HAVING COUNT(DISTINCT tp.post_business_key) > 0
            )
            SELECT 
                tag_name,
                post_count,
                comment_count,
                comment_count::float / NULLIF(post_count, 0) as comments_per_post,
                avg_response_time_seconds / 60 as avg_response_time_minutes
            FROM TagStats
            ORDER BY comment_count DESC
            LIMIT :limit
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"limit": limit})
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                
                if df.empty:
                    self.log_message("Tag engagement analysis returned no results", level=logging.WARNING)
                else:
                    self.log_message(f"Tag engagement analysis returned {len(df)} rows")
                
                return df
        except Exception as e:
            self.log_message(f"Error in tag engagement analysis: {str(e)}", level=logging.ERROR)
            return pd.DataFrame()

    def log_message(self, message, level=logging.INFO):
        """Helper function to log messages with a timestamp."""
        logging.log(level, message)
        print(f"{datetime.now()} - {logging.getLevelName(level)} - {message}")
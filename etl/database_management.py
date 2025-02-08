import os
import configparser
from dotenv import load_dotenv

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, LargeBinary, Text, Float, ForeignKey, select, text, inspect
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import logging
from datetime import datetime
import hashlib

class PostgreSQLConnector:
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

        # Create tables if they do not exist
        self._create_tables()

        # Create additional functions (e.g., sha256()) in the database
        self._create_functions()

        # Setup logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)


    def _create_engine(self):
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        engine = create_engine(url, echo=False)
        if not database_exists(engine.url):
            create_database(engine.url)
            print(f"✅ Initialized new database: {self.db_name}")
        else:
            print(f"✅ Connected to existing database: {self.db_name}")
        return engine

    def _create_tables(self):
        # Reflect and drop existing tables
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        if metadata.tables:
            print('Dropping existing tables:')
            metadata.drop_all(self.engine)
            print('Existing tables dropped.')
        
        # Reinitialize metadata for new table definitions
        metadata = MetaData()

        # Hub Tables
        hub_user = Table(
            'hub_user', metadata,
            Column('user_hk', LargeBinary, primary_key=True, nullable=False),
            Column('user_business_key', Integer, nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        hub_post = Table(
            'hub_post', metadata,
            Column('post_hk', LargeBinary, primary_key=True, nullable=False),
            Column('post_business_key', Integer, nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        hub_comment = Table(
            'hub_comment', metadata,
            Column('comment_hk', LargeBinary, primary_key=True, nullable=False),
            Column('comment_business_key', Integer, nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        hub_vote = Table(
            'hub_vote', metadata,
            Column('vote_hk', LargeBinary, primary_key=True, nullable=False),
            Column('vote_business_key', Integer, nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        hub_badge = Table(
            'hub_badge', metadata,
            Column('badge_hk', LargeBinary, primary_key=True, nullable=False),
            Column('badge_business_key', Integer, nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        hub_tag = Table(
            'hub_tag', metadata,
            Column('tag_hk', LargeBinary, primary_key=True, nullable=False),
            Column('tag_business_key', Integer, nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        # Link Tables
        link_post_user = Table(
            'link_post_user', metadata,
            Column('post_user_hk', LargeBinary, primary_key=True, nullable=False),
            Column('post_hk', LargeBinary, ForeignKey('hub_post.post_hk'), nullable=False),
            Column('user_hk', LargeBinary, ForeignKey('hub_user.user_hk'), nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        link_post_post = Table(
            'link_post_post', metadata,
            Column('post_post_hk', LargeBinary, primary_key=True, nullable=False),
            Column('source_post_hk', LargeBinary, ForeignKey('hub_post.post_hk'), nullable=False),
            Column('target_post_hk', LargeBinary, ForeignKey('hub_post.post_hk'), nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        link_post_comment = Table(
            'link_post_comment', metadata,
            Column('post_comment_hk', LargeBinary, primary_key=True, nullable=False),
            Column('post_hk', LargeBinary, ForeignKey('hub_post.post_hk'), nullable=False),
            Column('comment_hk', LargeBinary, ForeignKey('hub_comment.comment_hk'), nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        link_post_tag = Table(
            'link_post_tag', metadata,
            Column('post_tag_hk', LargeBinary, primary_key=True, nullable=False),
            Column('post_hk', LargeBinary, ForeignKey('hub_post.post_hk'), nullable=False),
            Column('tag_hk', LargeBinary, ForeignKey('hub_tag.tag_hk'), nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        link_user_vote = Table(
            'link_user_vote', metadata,
            Column('user_vote_hk', LargeBinary, primary_key=True, nullable=False),
            Column('user_hk', LargeBinary, ForeignKey('hub_user.user_hk'), nullable=False),
            Column('vote_hk', LargeBinary, ForeignKey('hub_vote.vote_hk'), nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        link_post_vote = Table(
            'link_post_vote', metadata,
            Column('post_vote_hk', LargeBinary, ForeignKey('hub_post.post_hk'), nullable=False),
            Column('vote_hk', LargeBinary, ForeignKey('hub_vote.vote_hk'), nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        link_user_badge = Table(
            'link_user_badge', metadata,
            Column('user_badge_hk', LargeBinary, primary_key=True, nullable=False),
            Column('user_hk', LargeBinary, ForeignKey('hub_user.user_hk'), nullable=False),
            Column('badge_hk', LargeBinary, ForeignKey('hub_badge.badge_hk'), nullable=False),
            Column('load_date', DateTime, nullable=False),
            Column('source', String(255), nullable=False)
        )

        # Satellite Tables
        sat_user = Table(
            'sat_user', metadata,
            Column('user_hk', LargeBinary, ForeignKey('hub_user.user_hk'), primary_key=True, nullable=False),
            Column('load_date', DateTime, primary_key=True, nullable=False),
            Column('reputation', Integer),
            Column('displayname', String(40)),
            Column('lastaccessdate', DateTime),
            Column('websiteurl', String(200)),
            Column('location', String(100)),
            Column('aboutme', String(800)),
            Column('views', Integer),
            Column('upvotes', Integer),
            Column('downvotes', Integer),
            Column('accountid', Integer),
            Column('profileimageurl', String(200)),
            Column('emailhash', String(32))
        )

        sat_post = Table(
            'sat_post', metadata,
            Column('post_hk', LargeBinary, ForeignKey('hub_post.post_hk'), primary_key=True, nullable=False),
            Column('load_date', DateTime, primary_key=True, nullable=False),
            Column('posttypeid', Integer),
            Column('score', Integer),
            Column('viewcount', Integer),
            Column('body', Text),
            Column('title', String(250)),
            Column('tags', String(250)),
            Column('answercount', Integer),
            Column('commentcount', Integer),
            Column('favoritecount', Integer),
            Column('owneruserid', Integer)
        )

        sat_comment = Table(
            'sat_comment', metadata,
            Column('comment_hk', LargeBinary, ForeignKey('hub_comment.comment_hk'), primary_key=True, nullable=False),
            Column('load_date', DateTime, primary_key=True, nullable=False),
            Column('score', Integer),
            Column('text', String(600)),
            Column('userdisplayname', String(30))
        )

        sat_vote = Table(
            'sat_vote', metadata,
            Column('vote_hk', LargeBinary, ForeignKey('hub_vote.vote_hk'), primary_key=True, nullable=False),
            Column('load_date', DateTime, primary_key=True, nullable=False),
            Column('votetypeid', Integer),
            Column('bountyamount', Integer)
        )

        sat_badge = Table(
            'sat_badge', metadata,
            Column('badge_hk', LargeBinary, ForeignKey('hub_badge.badge_hk'), primary_key=True, nullable=False),
            Column('load_date', DateTime, primary_key=True, nullable=False),
            Column('name', String(50))
        )

        sat_tag = Table(
            'sat_tag', metadata,
            Column('tag_hk', LargeBinary, ForeignKey('hub_tag.tag_hk'), primary_key=True, nullable=False),
            Column('load_date', DateTime, primary_key=True, nullable=False),
            Column('tagname', String(35)),
            Column('count', Integer),
            Column('excerptpostid', Integer),
            Column('wikipostid', Integer)
        )

        try:
            metadata.create_all(self.engine)
            self._verify_tables()
            print('✅ Tables created successfully!')
        except Exception as e:
            print(f'❌ Error creating tables: {e}')
            raise

    def get_all_tables(self):
        """Retrieve all table names in the database."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def select_values(self, table_name):
        """Retrieve all rows from the specified table."""
        metadata = MetaData(bind=self.engine)
        table = Table(table_name, metadata, autoload_with=self.engine)
        query = select(table)
        with self.engine.connect() as conn:
            return conn.execute(query).fetchall()

    def insert_data_frame(self, dataframe, table_name):
        """Insert data from a Pandas DataFrame into the specified table."""
        try:
            dataframe.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            print('Data inserted successfully into', table_name)
        except Exception as e:
            print(f'Error inserting data into {table_name}: {e}')

    def _create_functions(self):
        """Create SQL functions required for the Data Vault."""
        create_sha256_function_sql = """
        CREATE EXTENSION IF NOT EXISTS pgcrypto;
        CREATE OR REPLACE FUNCTION sha256(input text)
        RETURNS bytea AS $$
        BEGIN
            RETURN digest(input, 'sha256');
        END;
        $$ LANGUAGE plpgsql IMMUTABLE STRICT;
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_sha256_function_sql))
                conn.commit()
            print('✅ SHA-256 function created successfully.')
        except Exception as e:
            print(f'❌ Error creating SHA-256 function: {e}')

    def _verify_tables(self):
        """Verify that all required tables exist."""
        inspector = inspect(self.engine)
        existing_tables = inspector.get_table_names()
        required_tables = [
            'hub_user', 'hub_post', 'hub_comment', 'hub_vote', 'hub_badge', 'hub_tag',
            'link_post_user', 'link_post_comment', 'link_post_post', 'link_user_vote',
            'link_post_vote', 'link_user_badge',
            'sat_user', 'sat_post', 'sat_comment', 'sat_vote', 'sat_badge', 'sat_tag'
        ]
        missing_tables = [table for table in required_tables if table not in existing_tables]
        if missing_tables:
            raise Exception(f"Missing tables: {', '.join(missing_tables)}")

    def create_and_verify_tables(self):
         self._create_tables()

    def connect_to_db(self):
        """Connects to the PostgreSQL database and returns connection, now truncating tables with it."""
        try:
            conn = self.engine.connect() # Now returns a connection object from SQLAlchemy
            self.logger.info("Successfully connected to the database.")
            return conn
        except Exception as e:
            self.logger.error(f"Error connecting to the database: {e}", exc_info=True)
            raise

    def log_message(self, message, level=logging.INFO):
        """Helper function to log messages with a timestamp."""
        logging.log(level, message)
        print(f"{datetime.now()} - {logging.getLevelName(level)} - {message}")

    def sha256_hash(self, value):
        """Calculates the SHA-256 hash of a value."""
        if value is None: #Handle Null values
            return None
        return hashlib.sha256(str(value).encode('utf-8')).digest()

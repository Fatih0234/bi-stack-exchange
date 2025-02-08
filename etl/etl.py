import pandas as pd
import logging
from datetime import datetime
import numpy as np
import hashlib
import os
import psycopg2
from .database_management import PostgreSQLConnector

class DataVaultLoader:
    def __init__(self, db_connector, csv_directory="/app/stack-exchange-data"):
        self.db_connector = db_connector
        self.csv_directory = csv_directory
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def sha256_hash(self, value):
        """Calculates the SHA-256 hash of a value."""
        if value is None: #Handle Null values
            return None
        return hashlib.sha256(str(value).encode('utf-8')).digest()

    def load_data(self, conn, table_name, df, business_key_column, source_name):
        """Loads data into a specified Data Vault table."""
        cursor = conn.connection.cursor()
        self.log_message(f"Starting data load for table: {table_name} from source: {source_name}")
        self.log_message(f"Number of rows to process: {len(df)}")

        try:
            # Convert numpy int64/float64 to Python int
            for col in df.select_dtypes(include=['int64', 'float64']).columns:
                df[col] = df[col].astype(float).fillna(0).astype(int)

            # Convert all datetime columns to proper format
            date_columns = df.select_dtypes(include=['object']).columns
            for col in date_columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            rows_processed = 0
            for index, row in df.iterrows():
                try:
                    # Prepare values for insertion
                    values = []
                    columns = []

                    # Handle different table types
                    if table_name.startswith("hub_"):
                        # For Hub tables
                        hub_name = table_name[4:]  # Remove 'HUB_' prefix
                        business_key_value = int(row[business_key_column])
                        hk = self.sha256_hash(str(business_key_value))
                        columns = [f"{hub_name}_hk", f"{hub_name}_business_key", "load_date", "source"]
                        values = [hk, business_key_value, datetime.now(), source_name]

                    elif table_name.startswith("link_"):
                        # For Link tables
                        link_parts = table_name[5:].split('_')  # Remove 'LINK_' prefix and split
                        if len(link_parts) != 2:
                            raise ValueError(f"Invalid link table name format: {table_name}")
                        
                        # For link tables, we expect the hash keys to be pre-calculated
                        link_name = f"{link_parts[0]}_{link_parts[1]}"
                        
                        # Special case for link_post_post table
                        if table_name == "link_post_post":
                            required_columns = [
                                "post_post_hk",
                                "source_post_hk",
                                "target_post_hk"
                            ]
                            columns = ["post_post_hk", "source_post_hk", "target_post_hk", "load_date", "source"]
                            values = [
                                row["post_post_hk"],
                                row["source_post_hk"],
                                row["target_post_hk"],
                                datetime.now(),
                                source_name
                            ]
                        # Special case for link_post_vote table
                        elif table_name == "link_post_vote":
                            required_columns = [
                                "post_vote_hk",
                                "vote_hk"
                            ]
                            columns = ["post_vote_hk", "vote_hk", "load_date", "source"]
                            values = [
                                row["post_vote_hk"],
                                row["vote_hk"],
                                datetime.now(),
                                source_name
                            ]
                        else:
                            # For all other link tables
                            required_columns = [
                                f"{link_name}_hk",
                                f"{link_parts[0]}_hk",
                                f"{link_parts[1]}_hk"
                            ]
                            columns = [f"{link_name}_hk", f"{link_parts[0]}_hk", f"{link_parts[1]}_hk", "load_date", "source"]
                            values = [
                                row[f"{link_name}_hk"],
                                row[f"{link_parts[0]}_hk"],
                                row[f"{link_parts[1]}_hk"],
                                datetime.now(),
                                source_name
                            ]
                        
                        # Check if the required columns exist
                        for col in required_columns:
                            if col not in row:
                                self.log_message(f"Missing required column {col} in link table {table_name}", level=logging.ERROR)
                                self.log_message(f"Available columns: {list(row.index)}", level=logging.ERROR)
                                raise KeyError(f"Missing required column {col}")

                    elif table_name.startswith("sat_"):
                        # For Satellite tables
                        entity = table_name[4:]  # Remove 'SAT_' prefix
                        business_key_value = int(row[business_key_column])
                        hk = self.sha256_hash(str(business_key_value))
                        columns = [f"{entity}_hk", "load_date"]
                        values = [hk, datetime.now()]

                        # Define allowed columns for each satellite entity based on schema
                        allowed_columns = {
                            'user': ['reputation', 'displayname', 'lastaccessdate', 'websiteurl', 'location', 'aboutme', 'views', 'upvotes', 'downvotes', 'accountid', 'profileimageurl', 'emailhash'],
                            'post': ['posttypeid', 'score', 'viewcount', 'body', 'title', 'tags', 'answercount', 'commentcount', 'favoritecount', 'owneruserid'],
                            'comment': ['score', 'text', 'userdisplayname'],
                            'badge': ['name'],
                            'vote': ['votetypeid', 'bountyamount'],
                            'tag': ['tagname', 'count', 'excerptpostid', 'wikipostid']
                        }
                        allowed = allowed_columns.get(entity.lower(), None)

                        # Add allowed columns from the dataframe
                        for col in df.columns:
                            if col != business_key_column:
                                col_lower = col.lower()
                                if allowed is None or col_lower in allowed:
                                    # Define maximum lengths for string columns based on entity
                                    max_lengths = {
                                        'user': {'displayname': 40, 'websiteurl': 200, 'location': 100, 'aboutme': 800, 'profileimageurl': 200, 'emailhash': 32},
                                        'post': {'title': 250, 'tags': 250},
                                        'comment': {'text': 600, 'userdisplayname': 30},
                                        'badge': {'name': 50},
                                        'tag': {'tagname': 35},
                                        'vote': {}
                                    }
                                    entity_key = entity.lower()
                                    if pd.notnull(row[col]):
                                        if isinstance(row[col], (np.int64, np.float64)):
                                            value = int(row[col])
                                        elif isinstance(row[col], str):
                                            value = row[col].strip()
                                            if entity_key in max_lengths and col_lower in max_lengths[entity_key]:
                                                max_len = max_lengths[entity_key][col_lower]
                                                if len(value) > max_len:
                                                    value = value[:max_len]
                                        else:
                                            value = row[col]
                                        columns.append(col_lower)
                                        values.append(value)

                    if columns and values:  # Only proceed if we have data to insert
                        placeholders = ', '.join(['%s'] * len(values))
                        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                        try:
                            cursor.execute(query, values)
                            rows_processed += 1
                            if rows_processed % 10000 == 0:  # Log progress every 10000 rows
                                self.log_message(f"Processed {rows_processed} rows...")
                        except Exception as e:
                            self.log_message(f"Error executing query: {query}", level=logging.ERROR)
                            self.log_message(f"Values: {values}", level=logging.ERROR)
                            raise e

                except Exception as e:
                    self.log_message(f"Error processing row {index}: {e}", level=logging.ERROR)
                    raise e

            conn.connection.commit()
            self.log_message(f"Successfully loaded {rows_processed} rows into {table_name}")

        except psycopg2.Error as e:
            conn.connection.rollback()
            self.log_message(f"Error loading data into {table_name}: {e}", level=logging.ERROR)
            raise
        except Exception as e:
            conn.connection.rollback()
            self.log_message(f"Error during data loading for {table_name}: {e}", level=logging.ERROR)
            raise
        finally:
            cursor.close()

    def truncate_tables(self, conn):
        """Truncates all Data Vault tables to remove existing data."""
        cursor = conn.connection.cursor()
        try:
            table_names = [
                "hub_user", "hub_post", "hub_comment", "hub_vote", "hub_badge", "hub_tag",
                "link_post_user", "link_post_comment", "link_post_post", "link_user_vote", "link_post_vote", "link_user_badge",
                "sat_user", "sat_post", "sat_comment", "sat_vote", "sat_badge", "sat_tag"
            ]
            for table in table_names:
                try:
                    cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")  # CASCADE to handle foreign key dependencies
                    conn.connection.commit()  # Commit after each successful truncate
                    self.log_message(f"Successfully truncated table: {table}")
                except psycopg2.Error as e:
                    conn.connection.rollback()  # Rollback the failed truncate
                    if 'does not exist' in str(e):
                        self.log_message(f"Table {table} does not exist, skipping...", level=logging.WARNING)
                    else:
                        self.log_message(f"Error truncating table {table}: {e}", level=logging.ERROR)
                        raise
        finally:
            cursor.close()

    def log_message(self, message, level=logging.INFO):
        """Helper function to log messages with a timestamp."""
        logging.log(level, message)
        print(f"{datetime.now()} - {logging.getLevelName(level)} - {message}")

    def etl_users(self, conn):
        """ETL process for the Users table."""
        try:
            self.log_message("Starting ETL process for Users")
            file_path = os.path.join(self.csv_directory, "users.csv")
            df = pd.read_csv(file_path)

            # Transformation: Handle missing values and data types
            df.fillna(0, inplace=True)

            # Convert dates
            df['CreationDate'] = pd.to_datetime(df['CreationDate'])
            df['LastAccessDate'] = pd.to_datetime(df['LastAccessDate'])

            # Load data into HUB_USER
            df_hub = df[['Id']].copy()
            self.load_data(conn, "hub_user", df_hub, "Id", "users.csv")

            # Prepare satellite data with correct column names
            df_sat = df.copy()
            df_sat = df_sat.rename(columns={
                'Reputation': 'reputation',
                'DisplayName': 'displayname',
                'LastAccessDate': 'lastaccessdate',
                'WebsiteUrl': 'websiteurl',
                'Location': 'location',
                'AboutMe': 'aboutme',
                'Views': 'views',
                'UpVotes': 'upvotes',
                'DownVotes': 'downvotes',
                'AccountId': 'accountid',
                'ProfileImageUrl': 'profileimageurl',
            'EmailHash':'emailhash'
        })

            # Load data into SAT_USER
            self.load_data(conn, "sat_user", df_sat, "Id", "users.csv")

            self.log_message("Successfully completed ETL process for Users")

        except Exception as e:
            self.log_message(f"Error during ETL process for Users: {e}", level=logging.ERROR)

    def etl_posts(self, conn):
        """ETL process for the Posts table."""
        try:
            self.log_message("Starting ETL process for Posts")
            file_path = os.path.join(self.csv_directory, "posts.csv")
            df = pd.read_csv(file_path)

            # Fix column names
            df = df.rename(columns={
                'CreaionDate': 'CreationDate',
                'LasActivityDate': 'LastActivityDate'
            })

            # Transformation: Handle missing values and data types
            df['AcceptedAnswerId'].fillna(0, inplace=True)
            df['ViewCount'].fillna(0, inplace=True)
            df['Body'].fillna('', inplace=True)
            df['Title'].fillna('', inplace=True)
            df['Tags'].fillna('', inplace=True)
            df['AnswerCount'].fillna(0, inplace=True)
            df['FavoriteCount'].fillna(0, inplace=True)
            df['LastEditorUserId'].fillna(0, inplace=True)
            df['LastEditorDisplayName'].fillna('', inplace=True)
            df['OwnerDisplayName'].fillna('', inplace=True)
            df.fillna(0, inplace=True)

            # Convert dates
            date_columns = ['CreationDate', 'LastActivityDate', 'LastEditDate', 'CommunityOwnedDate', 'ClosedDate']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')

            # Load data into HUB_POST
            df_hub = df[['Id']].copy()
            self.load_data(conn, "hub_post", df_hub, "Id", "posts.csv")

            # Prepare satellite data with correct column names
            df_sat = df.copy()
            df_sat = df_sat.rename(columns={
                'PostTypeId': 'posttypeid',
                'Score': 'score',
                'ViewCount': 'viewcount',
                'Body': 'body',
                'Title': 'title',
                'Tags': 'tags',
                'AnswerCount': 'answercount',
                'CommentCount': 'commentcount',
                'FavoriteCount': 'favoritecount',
                'OwnerUserId': 'owneruserid'
            })

            # Load data into SAT_POST
            self.load_data(conn, "sat_post", df_sat, "Id", "posts.csv")

            # Load users data to verify existence
            users_file = os.path.join(self.csv_directory, "users.csv")
            df_users = pd.read_csv(users_file)
            valid_user_ids = set(df_users["Id"].astype(int))
            
            # Create a mapping of user IDs to their hash keys
            user_hash_mapping = {
                int(user_id): self.sha256_hash(str(user_id))
                for user_id in valid_user_ids
            }
            
            self.log_message(f"Created hash mapping for {len(user_hash_mapping)} users")
            
            # Prepare link data for LINK_POST_USER
            df_link = df[['Id', 'OwnerUserId']].copy()
            df_link = df_link.dropna()
            df_link = df_link[df_link['OwnerUserId'] != 0]
            
            # Filter for valid user relationships
            df_link = df_link[df_link['OwnerUserId'].astype(int).isin(valid_user_ids)]
            df_link['OwnerUserId'] = df_link['OwnerUserId'].astype(int)  # Keep as int for mapping
            
            self.log_message(f"Found {len(df_link)} valid post-user relationships")
            
            # Debug: Check some sample values
            sample_users = df_link['OwnerUserId'].head()
            self.log_message(f"Sample OwnerUserIds: {list(sample_users)}")
            self.log_message(f"Sample mapping keys: {list(user_hash_mapping.keys())[:5]}")
            
            if len(df_link) > 0:
                # Generate hash keys for the link table using the mapping for user_hk
                df_link['post_user_hk'] = df_link.apply(lambda x: self.sha256_hash(f"{x['Id']}_{x['OwnerUserId']}"), axis=1)
                df_link['post_hk'] = df_link['Id'].apply(lambda x: self.sha256_hash(str(x)))
                df_link['user_hk'] = df_link['OwnerUserId'].map(user_hash_mapping)
                
                # Debug: Check mapping results
                missing_mappings = df_link[df_link['user_hk'].isna()]
                if len(missing_mappings) > 0:
                    self.log_message(f"Sample of users with missing mappings: {list(missing_mappings['OwnerUserId'].head())}")
                    self.log_message(f"Their values exist in mapping: {[uid in user_hash_mapping for uid in missing_mappings['OwnerUserId'].head()]}")
                
                # Remove any rows where user_hk mapping failed
                df_link = df_link.dropna(subset=['user_hk'])
                
                # Log the columns for debugging
                self.log_message(f"Link table columns: {list(df_link.columns)}")
                self.log_message(f"Final number of valid relationships after hash mapping: {len(df_link)}")
                
                if len(df_link) > 0:
                    self.load_data(conn, "link_post_user", df_link, "Id", "posts.csv")
                else:
                    self.log_message("No valid relationships remained after hash mapping")
            else:
                self.log_message("No valid post-user relationships found to load")

            self.log_message("Successfully completed ETL process for Posts")

        except Exception as e:
            self.log_message(f"Error during ETL process for Posts: {e}", level=logging.ERROR)
            raise

    def etl_comments(self, conn):
        """ETL process for the Comments table."""
        try:
            self.log_message("Starting ETL process for Comments")
            file_path = os.path.join(self.csv_directory, "comments.csv")
            df = pd.read_csv(file_path)

            # Transformation: Handle missing values and data types
            df = df.rename(columns={'PostId': 'PostID','UserId': 'UserId'})
            df['Text'].fillna('', inplace=True)
            df['UserDisplayName'].fillna('', inplace=True)
            df.fillna(0, inplace=True)

            # Convert dates
            df['CreationDate'] = pd.to_datetime(df['CreationDate'])

            # Load data into HUB_COMMENT
            df_hub = df[['Id']].copy()
            df_hub = df_hub.dropna()
            self.load_data(conn, "hub_comment", df_hub, "Id", "comments.csv")

            # Prepare satellite data with correct column names
            df_sat = df.copy()
            df_sat = df_sat.rename(columns={
                'Score': 'score',
                'Text': 'text',
                'UserDisplayName': 'userdisplayname'
            })

            # Load data into SAT_COMMENT
            self.load_data(conn, "sat_comment", df_sat, "Id", "comments.csv")

            # Create link_post_comment relationships
            # First verify that the posts exist
            posts_file = os.path.join(self.csv_directory, "posts.csv")
            df_posts = pd.read_csv(posts_file)
            valid_post_ids = set(df_posts["Id"].astype(int))
            
            # Create post-comment links
            df_link = df[['Id', 'PostID']].copy()
            df_link = df_link.dropna()
            df_link = df_link[df_link['PostID'] != 0]
            
            # Filter for valid post relationships
            df_link = df_link[df_link['PostID'].astype(int).isin(valid_post_ids)]
            
            self.log_message(f"Found {len(df_link)} valid post-comment relationships")
            
            if len(df_link) > 0:
                # Generate hash keys for the link table
                df_link['post_comment_hk'] = df_link.apply(lambda x: self.sha256_hash(f"{x['PostID']}_{x['Id']}"), axis=1)
                df_link['post_hk'] = df_link['PostID'].apply(lambda x: self.sha256_hash(str(x)))
                df_link['comment_hk'] = df_link['Id'].apply(lambda x: self.sha256_hash(str(x)))
                
                # Load the link data
                self.load_data(conn, "link_post_comment", df_link, "Id", "comments.csv")
            else:
                self.log_message("No valid post-comment relationships found to load")

            self.log_message("Successfully completed ETL process for Comments")

        except Exception as e:
            self.log_message(f"Error during ETL process for Comments: {e}", level=logging.ERROR)
            raise

    def etl_badges(self, conn):
        """ETL process for the Badges table."""
        try:
            self.log_message("Starting ETL process for Badges")
            file_path = os.path.join(self.csv_directory, "badges.csv")
            df = pd.read_csv(file_path)

            # Transformation: Handle missing values and data types
            df['Name'].fillna('', inplace=True)
            df.fillna(0, inplace=True)

            # Convert dates
            df['Date'] = pd.to_datetime(df['Date'])

            # Load data into HUB_BADGE
            df_hub = df[['Id']].copy()
            self.load_data(conn, "hub_badge", df_hub, "Id", "badges.csv")

            # Prepare satellite data with correct column names
            df_sat = df.copy()
            df_sat = df_sat.rename(columns={
                'Name': 'name'
            })

            # Load data into SAT_BADGE
            self.load_data(conn, "sat_badge", df_sat, "Id", "badges.csv")

            # Create link_user_badge relationships
            # First verify that the users exist
            users_file = os.path.join(self.csv_directory, "users.csv")
            df_users = pd.read_csv(users_file)
            valid_user_ids = set(df_users["Id"].astype(int))
            
            # Create user-badge links
            df_link = df[['Id', 'UserId']].copy()
            df_link = df_link.dropna()
            df_link = df_link[df_link['UserId'] != 0]
            
            # Filter for valid user relationships
            df_link = df_link[df_link['UserId'].astype(int).isin(valid_user_ids)]
            
            self.log_message(f"Found {len(df_link)} valid user-badge relationships")
            
            if len(df_link) > 0:
                # Generate hash keys for the link table
                df_link['user_badge_hk'] = df_link.apply(lambda x: self.sha256_hash(f"{x['UserId']}_{x['Id']}"), axis=1)
                df_link['user_hk'] = df_link['UserId'].apply(lambda x: self.sha256_hash(str(x)))
                df_link['badge_hk'] = df_link['Id'].apply(lambda x: self.sha256_hash(str(x)))
                
                # Load the link data
                self.load_data(conn, "link_user_badge", df_link, "Id", "badges.csv")
            else:
                self.log_message("No valid user-badge relationships found to load")

            self.log_message("Successfully completed ETL process for Badges")

        except Exception as e:
            self.log_message(f"Error during ETL process for Badges: {e}", level=logging.ERROR)
            raise

    def etl_votes(self, conn):
        """ETL process for the Votes table."""
        try:
            self.log_message("Starting ETL process for Votes")
            file_path = os.path.join(self.csv_directory, "votes.csv")
            df = pd.read_csv(file_path)

            # Transformation: Handle missing values and data types
            df = df.rename(columns={'PostId': 'PostID','UserId': 'UserId'})
            df.fillna(0, inplace=True)

            # Convert dates
            df['CreationDate'] = pd.to_datetime(df['CreationDate'])

            # Load data into HUB_VOTE
            df_hub = df[['Id']].copy()
            self.load_data(conn, "hub_vote", df_hub, "Id", "votes.csv")

            # Prepare satellite data with correct column names
            df_sat = df.copy()
            df_sat = df_sat.rename(columns={
                'VoteTypeId': 'votetypeid',
                'CreationDate': 'creationdate',
                'PostId': 'postid',
                'UserId': 'userid',
                'BountyAmount': 'bountyamount'
            })

            # Load data into SAT_VOTE
            self.load_data(conn, "sat_vote", df_sat, "Id", "votes.csv")

            # Create link_user_vote relationships
            # First verify that the users exist
            users_file = os.path.join(self.csv_directory, "users.csv")
            df_users = pd.read_csv(users_file)
            valid_user_ids = set(df_users["Id"].astype(int))
            
            # Create a mapping of user IDs to their hash keys
            user_hash_mapping = {
                int(user_id): self.sha256_hash(str(user_id))
                for user_id in valid_user_ids
            }
            
            # Create user-vote links
            df_user_vote_link = df[['Id', 'UserId']].copy()
            df_user_vote_link = df_user_vote_link.dropna()
            df_user_vote_link = df_user_vote_link[df_user_vote_link['UserId'] != 0]
            df_user_vote_link['UserId'] = df_user_vote_link['UserId'].astype(int)  # Convert to int for mapping
            
            # Filter for valid user relationships
            df_user_vote_link = df_user_vote_link[df_user_vote_link['UserId'].isin(valid_user_ids)]
            
            self.log_message(f"Found {len(df_user_vote_link)} valid user-vote relationships")
            
            if len(df_user_vote_link) > 0:
                # Generate hash keys for the link table using the mapping for user_hk
                df_user_vote_link['user_vote_hk'] = df_user_vote_link.apply(lambda x: self.sha256_hash(f"{x['UserId']}_{x['Id']}"), axis=1)
                df_user_vote_link['vote_hk'] = df_user_vote_link['Id'].apply(lambda x: self.sha256_hash(str(x)))
                df_user_vote_link['user_hk'] = df_user_vote_link['UserId'].map(user_hash_mapping)
                
                # Debug: Check mapping results
                missing_mappings = df_user_vote_link[df_user_vote_link['user_hk'].isna()]
                if len(missing_mappings) > 0:
                    self.log_message(f"Sample of users with missing mappings: {list(missing_mappings['UserId'].head())}")
                    self.log_message(f"Their values exist in mapping: {[uid in user_hash_mapping for uid in missing_mappings['UserId'].head()]}")
                
                # Remove any rows where user_hk mapping failed
                df_user_vote_link = df_user_vote_link.dropna(subset=['user_hk'])
                
                # Log the columns for debugging
                self.log_message(f"Link table columns: {list(df_user_vote_link.columns)}")
                self.log_message(f"Final number of valid relationships after hash mapping: {len(df_user_vote_link)}")
                
                if len(df_user_vote_link) > 0:
                    # Load the user-vote link data
                    self.load_data(conn, "link_user_vote", df_user_vote_link, "Id", "votes.csv")
                else:
                    self.log_message("No valid relationships remained after hash mapping")
            else:
                self.log_message("No valid user-vote relationships found to load")

            # Create link_post_vote relationships
            # First verify that the posts exist
            posts_file = os.path.join(self.csv_directory, "posts.csv")
            df_posts = pd.read_csv(posts_file)
            valid_post_ids = set(df_posts["Id"].astype(int))
            
            # Create post-vote links
            df_post_vote_link = df[['Id', 'PostID']].copy()
            df_post_vote_link = df_post_vote_link.dropna()
            df_post_vote_link['PostID'] = df_post_vote_link['PostID'].astype(int)  # Convert to int for consistency
            
            # Filter for valid post relationships
            df_post_vote_link = df_post_vote_link[df_post_vote_link['PostID'].isin(valid_post_ids)]
            
            self.log_message(f"Found {len(df_post_vote_link)} valid post-vote relationships")
            
            if len(df_post_vote_link) > 0:
                # Generate hash keys for the link table
                df_post_vote_link['post_vote_hk'] = df_post_vote_link['PostID'].apply(lambda x: self.sha256_hash(str(x)))
                df_post_vote_link['vote_hk'] = df_post_vote_link['Id'].apply(lambda x: self.sha256_hash(str(x)))
                
                # Log the columns for debugging
                self.log_message(f"Link table columns: {list(df_post_vote_link.columns)}")
                self.log_message(f"Final number of valid post-vote relationships: {len(df_post_vote_link)}")
                
                # Load the post-vote link data
                self.load_data(conn, "link_post_vote", df_post_vote_link, "Id", "votes.csv")
            else:
                self.log_message("No valid post-vote relationships found to load")

            self.log_message("Successfully completed ETL process for Votes")

        except Exception as e:
            self.log_message(f"Error during ETL process for Votes: {e}", level=logging.ERROR)
            raise

    def etl_tags(self, conn):
        """ETL process for the Tags table."""
        try:
            self.log_message("Starting ETL process for Tags")
            file_path = os.path.join(self.csv_directory, "tags.csv")
            df = pd.read_csv(file_path)
            
            self.log_message(f"Loaded {len(df)} rows from tags.csv")

            # Transformation: Handle missing values and data types
            df['TagName'].fillna('', inplace=True)
            df['Count'].fillna(0, inplace=True)
            df['ExcerptPostId'].fillna(0, inplace=True)
            df['WikiPostId'].fillna(0, inplace=True)

            # Load data into HUB_TAG
            df_hub = df[['Id']].copy()
            self.log_message(f"Preparing to load {len(df_hub)} rows into hub_tag")
            self.load_data(conn, "hub_tag", df_hub, "Id", "tags.csv")

            # Prepare satellite data with correct column names
            df_sat = df.copy()
            df_sat = df_sat.rename(columns={
                'TagName': 'tagname',
                'Count': 'count',
                'ExcerptPostId': 'excerptpostid',
                'WikiPostId': 'wikipostid'
            })
            
            self.log_message(f"Preparing to load {len(df_sat)} rows into sat_tag")
            self.load_data(conn, "sat_tag", df_sat, "Id", "tags.csv")

            self.log_message("Successfully completed ETL process for Tags")

        except Exception as e:
            self.log_message(f"Error during ETL process for Tags: {e}", level=logging.ERROR)
            raise  # Re-raise the exception to ensure it's properly handled

    def etl_post_links(self, conn):
        """ETL process for the PostLinks table."""
        try:
            self.log_message("Starting ETL process for PostLinks")
            file_path = os.path.join(self.csv_directory, "postLinks.csv")
            df = pd.read_csv(file_path)
            
            self.log_message(f"Loaded {len(df)} rows from postLinks.csv")

            # Handle missing values
            df.fillna(0, inplace=True)
            
            # Ensure all IDs are integers
            df['Id'] = df['Id'].astype(int)
            df['PostId'] = df['PostId'].astype(int)
            df['RelatedPostId'] = df['RelatedPostId'].astype(int)
            
            # Verify that the related posts exist in the posts table
            posts_file = os.path.join(self.csv_directory, "posts.csv")
            df_posts = pd.read_csv(posts_file)
            valid_post_ids = set(df_posts["Id"].astype(int))
            
            # Filter for valid post relationships
            df = df[df["PostId"].isin(valid_post_ids) & df["RelatedPostId"].isin(valid_post_ids)]
            
            self.log_message(f"Found {len(df)} valid post relationships")
            
            # Generate hash keys for the link table
            df_link = pd.DataFrame()
            df_link['post_post_hk'] = df.apply(lambda x: self.sha256_hash(f"{x['PostId']}_{x['RelatedPostId']}"), axis=1)
            df_link['source_post_hk'] = df['PostId'].apply(lambda x: self.sha256_hash(str(x)))
            df_link['target_post_hk'] = df['RelatedPostId'].apply(lambda x: self.sha256_hash(str(x)))
            df_link['PostId'] = df['PostId']  # Keep original IDs for reference
            df_link['RelatedPostId'] = df['RelatedPostId']
            
            df_link = df_link.drop_duplicates(subset=['PostId', 'RelatedPostId'])
            
            self.log_message(f"Preparing to load {len(df_link)} rows into link_post_post")
            
            # Load the link data
            self.load_data(conn, "link_post_post", df_link, "PostId", "postLinks.csv")
            
            self.log_message("Successfully completed ETL process for PostLinks")

        except Exception as e:
            self.log_message(f"Error during ETL process for PostLinks: {e}", level=logging.ERROR)
            raise

    def run_etl(self, conn):
        """Main function to execute the ETL processes."""
        try:
            self.log_message("Starting ETL process")
            self.log_message("Step 1: Truncating existing tables")
            self.truncate_tables(conn)

            # Step 2: Load Hub tables first
            self.log_message("Step 2: Loading Hub tables")
            
            self.log_message("Loading Tags hub and satellite")
            self.etl_tags(conn)
            
            self.log_message("Loading Users hub and satellite")
            self.etl_users(conn)
            
            self.log_message("Loading Posts hub and satellite")
            self.etl_posts(conn)
            
            self.log_message("Loading Comments hub and satellite")
            self.etl_comments(conn)
            
            self.log_message("Loading Badges hub and satellite")
            self.etl_badges(conn)
            
            self.log_message("Loading Votes hub and satellite")
            self.etl_votes(conn)

            # Step 3: Load Link tables
            self.log_message("Step 3: Loading Link tables")
            
            self.log_message("Loading Post-Tag links")
            self.etl_post_links(conn)

            # Verify data loaded correctly
            self.log_message("Step 4: Verifying data loaded correctly")
            cursor = conn.connection.cursor()
            
            # Check hub tables
            for table in ['hub_user', 'hub_post', 'hub_comment', 'hub_vote', 'hub_badge', 'hub_tag']:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                self.log_message(f"{table} count: {count}")
            
            # Check link tables
            for table in ['link_post_user', 'link_post_comment', 'link_post_post', 'link_user_vote', 'link_post_vote', 'link_user_badge']:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                self.log_message(f"{table} count: {count}")
            
            # Check satellite tables
            for table in ['sat_user', 'sat_post', 'sat_comment', 'sat_vote', 'sat_badge', 'sat_tag']:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                self.log_message(f"{table} count: {count}")

            cursor.close()
            self.log_message("All ETL processes completed successfully!")

        except Exception as e:
            self.log_message(f"A fatal error occurred during ETL: {e}", level=logging.ERROR)
            raise
        finally:
            if conn:
                conn.close()
                self.log_message("Database connection closed.")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    try:
        # Initialize database connection
        db_connector = PostgreSQLConnector()
        conn = db_connector.connect_to_db()
        
        # Initialize and run ETL process
        etl = DataVaultLoader(db_connector)
        etl.truncate_tables(conn)  # Clear existing data
        etl.run_etl(conn)  # Run the ETL process
        
        logger.info("ETL process completed successfully")
    except Exception as e:
        logger.error(f"Error during ETL process: {e}")
        raise
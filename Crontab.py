import pandas as pd
from datetime import datetime, timedelta

"""
for "today", get the previous date (m/d/y)
if the previous date file does not exist (aka first run)
    query via now() - 1d.
    store result in pkl file called gossip_crds_sample_<date>.parquet

if the previous file DOES exist
    read in previous date file: gossip_crds_sample_<date-1>.parquet
    query via now() - 1d
    from these two dataframes, check for duplicate rows.
    delete duplicate rows from the newly loaded data.
    write new dataframe to parquet file.

For processing data (when we have enough...)
# Process each chunk
for i in range(num .parquet files in crontab_data)
    chunk = pd.read_pickle(f'chunk_{i}.pkl')
    # Process the chunk
    print(chunk.head())  # Example processing step

"""

class Crontab:
    def __init__(self):
        self.df_now = None
        self.df_previous = None
        self.df_n_days_ago = None

    @staticmethod
    def get_timeslot():
        current_hour = datetime.now().hour
        if current_hour < 12:
            return "AM"
        elif current_hour >= 12 and current_hour <= 23:
            return "PM"
        else:
            print(f"ERROR: unexpected timeslot: {current_hour}")
            return None

    @staticmethod
    def build_filename(date, timeslot):
        return "/home/sol/gossip_crds_sample_data/gossip_crds_sample_" + date + "_" + timeslot + ".parquet"

    @staticmethod
    def get_filename_now():
        today = datetime.today().strftime('%m_%d_%Y')
        return Crontab.build_filename(today, Crontab.get_timeslot())

    @staticmethod
    def get_filename_previous():
        current_timeslot = Crontab.get_timeslot()
        if current_timeslot == "AM": # previous is going to be yesterday
            yesterday = datetime.today() - timedelta(days=1)
            yesterday = yesterday.strftime('%m_%d_%Y')
            return Crontab.build_filename(yesterday, "PM")
        elif current_timeslot == "PM": # previous timestamp is earlier today
            today = datetime.today().strftime('%m_%d_%Y')
            return Crontab.build_filename(today, "AM")
        else:
            print(f"ERROR! current timeslot is invalid: {current_timeslot}")
            return None

    @staticmethod
    def get_filename_n_days_ago(n):
        today = datetime.today()
        n_days_ago = today - timedelta(days=n)
        return Crontab.build_filename(n_days_ago.strftime('%m_%d_%Y'))

    def build_df_now(self, data):
        data_dict = [{
            'timestamp': sample.timestamp,
            'origin': sample.origin,
            'source': sample.source,
            'signature': sample.signature,
            'host_id': sample.host_id[:8] # only keep 8 chars of host id
        } for sample in data]

        self.df_now = pd.DataFrame(data_dict)

    def read_df_n_days_ago(self, n):
        try:
            self.df_n_days_ago = pd.read_parquet(Crontab.get_filename_n_days_ago(n))
        except FileNotFoundError:
            print(f"The file '{Crontab.get_filename_n_days_ago(n)}' does not exist. This is a first run")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def read_previous_df(self):
        try:
            self.df_previous = pd.read_parquet(Crontab.get_filename_previous())
        except FileNotFoundError:
            print(f"The file '{Crontab.get_filename_previous()}' does not exist. This is a first run")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    """
    These queries can be large: ~167M, so we compress them
    """
    def write_df_now_to_file(self):
        if self.df_now is not None:
            self.df_now.to_parquet(Crontab.get_filename_now(), compression='gzip')
        else:
            print("df_today is none.")

    """
    These queries can be large: ~167M, so we compress them
    """
    def write_df_n_days_ago_to_file(self, n):
        if self.df_now is not None:
            self.df_now.to_parquet(Crontab.get_filename_n_days_ago(n), compression='gzip')
        else:
            print("df_today is none.")

    def df_previous_exists(self):
        if self.df_previous is not None:
            return True
        return False

    def df_n_days_ago_exists(self):
        if self.df_n_days_ago is not None:
            return True
        return False

    def drop_duplicates_from_df_now(self):
        # Concatenate the two DataFrames with an additional column to distinguish them
        self.df_previous['DataFrame_ID'] = 'previous'
        self.df_now['DataFrame_ID'] = 'now'

        combined = pd.concat([self.df_previous, self.df_now])
        duplicates_mask = combined.duplicated(subset=['timestamp', 'origin', 'source', 'signature', 'host_id'], keep='first')

        # Filter out duplicates from 'today' by using the negation of the duplicates mask
        # and ensuring we're selecting rows that originally belonged to 'today'
        cleaned_today = combined[~duplicates_mask & (combined['DataFrame_ID'] == 'now')]

        # Drop DataFrame_ID column
        self.df_now = cleaned_today.drop(columns=['DataFrame_ID']) #.reset_index(drop=True, inplace=True)

    def drop_duplicates_from_current_df(self):
        # Concatenate the two DataFrames with an additional column to distinguish them
        self.df_n_days_ago['DataFrame_ID'] = 'previous'
        self.df_now['DataFrame_ID'] = 'now'

        combined = pd.concat([self.df_n_days_ago, self.df_now])
        duplicates_mask = combined.duplicated(subset=['timestamp', 'origin', 'source', 'signature', 'host_id'], keep='first')

        # Filter out duplicates from 'today' by using the negation of the duplicates mask
        # and ensuring we're selecting rows that originally belonged to 'today'
        cleaned_today = combined[~duplicates_mask & (combined['DataFrame_ID'] == 'now')]

        # Drop DataFrame_ID column
        self.df_now = cleaned_today.drop(columns=['DataFrame_ID']) #.reset_index(drop=True, inplace=True)

    def get_df_today(self):
        return self.df_now

    def get_df_now_size(self):
        return self.df_now.shape

    def get_df_yesterday(self):
        return self.df_previous

    def get_df_yesterday_size(self):
        return self.df_previous.shape

    def reset_dfs(self):
        self.df_now = None
        self.df_previous = None
        self.df_n_days_ago = None
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

# # Set option to display all rows
# pd.set_option('display.max_rows', None)

# # Set option to display all columns
# pd.set_option('display.max_columns', None)

class Crontab:
    def __init__(self):
        self.df_today = None
        self.df_yesterday = None
        # print(self.df)

    @staticmethod
    def build_filename(date):
        return "crontab_data/gossip_crds_sample_" + date + ".parquet"

    @staticmethod
    def get_filename_today():
        today = datetime.today().strftime('%m_%d_%Y')
        return Crontab.build_filename(today)

    @staticmethod
    def get_filename_yesterday():
        # Get today's date
        yesterday = datetime.today() - timedelta(days=1)
        yesterday = yesterday.strftime('%m_%d_%Y')
        return Crontab.build_filename(yesterday)

    def build_df_today(self, data):
        data_dict = [{
            'timestamp': sample.timestamp,
            'origin': sample.origin,
            'source': sample.source,
            'signature': sample.signature,
            'host_id': sample.host_id[:8] # only keep 8 chars of host id
        } for sample in data]

        self.df_today = pd.DataFrame(data_dict)

    def read_df_yesterday(self):
        try:
            # Attempt to read the Parquet file into a DataFrame
            self.df_yesterday = pd.read_parquet(Crontab.get_filename_yesterday())
            # Display the first few rows of the DataFrame
            # print(self.df_yesterday.head())
        except FileNotFoundError:
            print(f"The file '{Crontab.get_filename_yesterday()}' does not exist. This is a first run")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def write_df_to_file(self):
        # self.df.to_csv(filename, index=False, compression='gzip')
        # self.df.to_pickle('filename.pkl.gz', compression='gzip')
        if self.df_today is not None:
            self.df_today.to_parquet(Crontab.get_filename_today(), compression='gzip')
        else:
            print("df_today is none.")

    def df_yesterday_exists(self):
        if self.df_yesterday is not None:
            return True
        return False

    def drop_duplicates_from_today(self):
        # Step 1: Concatenate the two DataFrames with an additional column to distinguish them
        self.df_yesterday['DataFrame_ID'] = 'yesterday'
        self.df_today['DataFrame_ID'] = 'today'

        combined = pd.concat([self.df_yesterday, self.df_today])
        duplicates_mask = combined.duplicated(subset=['timestamp', 'origin', 'source', 'signature', 'host_id'], keep='first')

        # Filter out duplicates from 'today' by using the negation of the duplicates mask
        # and ensuring we're selecting rows that originally belonged to 'today'
        cleaned_today = combined[~duplicates_mask & (combined['DataFrame_ID'] == 'today')]

        # Drop DataFrame_ID column 
        self.df_today = cleaned_today.drop(columns=['DataFrame_ID']) #.reset_index(drop=True, inplace=True)

    def get_df_today(self):
        return self.df_today

    def get_df_today_size(self):
        return self.df_today.shape

    def get_df_yesterday(self):
        return self.df_yesterday

    def get_df_yesterday_size(self):
        return self.df_yesterday.shape
import pandas as pd
import matplotlib.pyplot as plt

class DuplicatePush:
    def __init__(self, influx):
        self.influx = influx

    def run(self):
        print("running duplicate push analysis")
        duplicate_push_counts = self.run_dup_push_count()
        df_dup = self.process_results(duplicate_push_counts)

        all_push_counts = self.run_all_push_success()
        df_push = self.process_results(all_push_counts)

        all_push_fail_counts = self.run_all_push_fail()
        df_push_fail = self.process_results(all_push_fail_counts)

        combined_df = self.build_df(df_dup, df_push, df_push_fail)
        frac_df = self.calc_fraction_dup_push(combined_df)
        print("frac_df")
        print(frac_df)

        print("plotting...")
        self.plot(frac_df)

    def build_df(self, df_dup, df_push, df_push_fail):
        combined_df = pd.concat([df_dup, df_push, df_push_fail], axis=1, join='inner')
        combined_df.columns = ['mean_num_duplicate_push_messages', 'mean_all_push_success', 'mean_all_push_fail']
        return combined_df

    def calc_fraction_dup_push(self, df):
        df['total_push'] = df['mean_all_push_success'] + df['mean_all_push_fail']
        df['fraction_dup_push'] = df['mean_num_duplicate_push_messages'] / df['total_push']
        return df

    def run_dup_push_count(self):
        return self.influx.query_num_duplicate_push_messages()

    def run_all_push_success(self):
        return self.influx.query_all_push_success()

    def run_all_push_fail(self):
        return self.influx.query_all_push_fail()

    def process_results(self, results):
        points = list(results.get_points())
        if not points:
            return None
        df = pd.DataFrame(points)
        df.set_index('time', inplace=True)
        return df

    def plot(self, frac_df):
        # Plot the ratio as a function of time
        plt.figure(figsize=(60, 20))
        plt.plot(frac_df.index, frac_df['fraction_dup_push'], marker='o', linestyle='-')
        plt.xlabel('Time')
        plt.ylabel('Fraction of Duplicate Push Messages')
        plt.title('Fraction of Duplicate Push Messages over all received push messages')
        plt.grid(True)
        plt.xticks(rotation=75)
        plt.margins(x=0.005, tight=True)
        plt.tight_layout()
        # plt.show()
        plt.savefig(f'plots/fraction_duplicate_push.png')




    

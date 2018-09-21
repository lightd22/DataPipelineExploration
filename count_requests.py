import luigi
import sqlite3
import pandas as pd

class count_requests(luigi.ExternalTask):
    path_to_db = luigi.Parameter(default="data/interim/db.sqlite")

    def run(self):
        conn = sqlite3.connect(self.path_to_db)
        query = "SELECT request_type, status, COUNT(*) as count FROM logs GROUP BY request_type, status;"
        df = pd.read_sql_query(query, conn)
        conn.close()

        request_totals = df.groupby('request_type')['count'].sum()
        df['request_rate'] = df.apply(lambda row: row['count']/request_totals[row['request_type']],axis=1)

        print(df)
        pickle = df.to_pickle(self.output().path)

    def output(self):
        return luigi.LocalTarget("data/processed/request_counts.pickle")

class request_failure_rate(luigi.Task):
    def requires(self):
        return count_requests()

    def output(self):
        return luigi.LocalTarget("data/processed/request_failure_rate.pickle")

    def run(self):
        with self.input().open('r') as infile:
            df = pd.read_pickle(self.input().path)
        failure_rates = df.loc[df['status'].isin([401,404])].groupby(['request_type'])['request_rate'].sum()

        print(failure_rates)
        failure_rates.to_pickle(self.output().path)

if __name__ == "__main__":
    luigi.run(["request_failure_rate", "--local-scheduler"])

from src.data.generate_logs import generate_logs
from src.data.store_logs import store_logs
import luigi

class generate_data(luigi.ExternalTask):
    path_to_logs = luigi.Parameter(default="data/raw/")
    max_log_entries = luigi.IntParameter(default=100)

    def output(self):
        return luigi.LocalTarget("data/raw/log_1.txt")

    def run(self):
        _ = generate_logs(directory=self.path_to_logs, LOG_MAX=self.max_log_entries)
        return

class process_logs(luigi.Task):
    path_to_logs = luigi.Parameter(default="data/raw/")
    path_to_db = luigi.Parameter(default="data/interim/db.sqlite")

    def requires(self):
        return generate_data(self.path_to_logs)

    def output(self):
        return luigi.LocalTarget(self.path_to_db)

    def run(self):
        _ = store_logs(data_path=self.path_to_logs, output_path=self.path_to_db)
        return

if __name__ == "__main__":

    luigi.run(["process_logs", "--local-scheduler"])

import time
import sys
import sqlite3
from datetime import datetime

def create_table(path='data/interim/db.sqlite'):
    conn = sqlite3.connect(path)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS logs (
      raw_log TEXT NOT NULL UNIQUE,
      remote_addr TEXT,
      time_local TEXT,
      request_type TEXT,
      request_path TEXT,
      status INTEGER,
      body_bytes_sent INTEGER,
      http_referer TEXT,
      http_user_agent TEXT,
      created DATETIME DEFAULT CURRENT_TIMESTAMP
      )
    """)
    conn.close()
    return True

def parse_line(line):
    split_line = line.split(" ")

    # This seems brittle. Is there a better way to parse log without requiring expert knowledge on it's format?
    if len(split_line) < 12:
        return []
    remote_addr = split_line[0]
    time_local = split_line[3] + " " + split_line[4]
    request_type = split_line[5].strip("\"")
    request_path = split_line[6]
    status = split_line[8]
    body_bytes_sent = split_line[9]
    http_referer = split_line[10]
    http_user_agent = " ".join(split_line[11:])
    created = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    return [
        remote_addr,
        time_local,
        request_type,
        request_path,
        status,
        body_bytes_sent,
        http_referer,
        http_user_agent,
        created
    ]

def insert_record(path='data/interim/db.sqlite', line='', parsed=[]):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    args = [line] + parsed
    cur.execute('INSERT INTO logs VALUES (?,?,?,?,?,?,?,?,?,?)', args)
    conn.commit()
    conn.close()
    return True

def store_logs(data_path='', output_path=''):
    LOG_FILE_1 = data_path + "log_1.txt"
    LOG_FILE_2 = data_path + "log_2.txt"

    _ = create_table(path=output_path)
    try:
        log_1 = open(LOG_FILE_1, 'r')
        log_2 = open(LOG_FILE_2, 'r')
        while True:
            log_1_loc = log_1.tell()
            log_1_line = log_1.readline()

            log_2_loc = log_2.tell()
            log_2_line = log_2.readline()

            if not log_1_line and not log_2_line:
                # At the end of both log files, sleep and check again
                time.sleep(1)
                log_1.seek(log_1_loc)
                log_2.seek(log_2_loc)
                break
            else:
                # New line found, parse and insert into DB
                if log_1_line:
                    line = log_1_line
                else:
                    line = log_2_line

                line = line.strip()
                parsed = parse_line(line)
                if len(parsed) > 0:
                    insert_record(output_path, line, parsed)

    except Exception as e:
        print(e)
    finally:
        log_1.close()
        log_2.close()
    return True

if __name__ == "__main__":
    create_table()
    try:
        log_1 = open(LOG_FILE_1, 'r')
        log_2 = open(LOG_FILE_2, 'r')
        while True:
            log_1_loc = log_1.tell()
            log_1_line = log_1.readline()

            log_2_loc = log_2.tell()
            log_2_line = log_2.readline()

            if not log_1_line and not log_2_line:
                # At the end of both log files, sleep and check again
                time.sleep(1)
                log_1.seek(log_1_loc)
                log_2.seek(log_2_loc)
                continue
            else:
                # New line found, parse and insert into DB
                if log_1_line:
                    line = log_1_line
                else:
                    line = log_2_line

                line = line.strip()
                parsed = parse_line(line)
                if len(parsed) > 0:
                    insert_record(line, parsed)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(e)
    finally:
        log_1.close()
        log_2.close()
        sys.exit()

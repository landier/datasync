import argparse
import sys
from time import sleep

import arrow
import schedule

from api_client import APIClient
from dal import DAL


class DataSync:
    def __init__(self):
        self.dal = DAL()
        self.client = APIClient()
    
    def main(self):
        #TODO: Add optional start and end date for the full sync in order to be able to re-play a sync should we have an issue
        parser = argparse.ArgumentParser(description="DataSync: fetch data from API to Snowflake")
        mode_group = parser.add_argument("mode", choices=["full", "daemon"], help="""'full' triggers a full sync of the data at once wheareas
                                                                                    'daemon' starts an incremental hourly sync""")
        args = parser.parse_args()
        if args.mode == "full":
            self.full_sync()
        else:
            self.start_daemon()

    def start_daemon(self):
        print("Daemon started")
        schedule.every().minute.at(":00").do(self.incremental_sync)
        try:
            while 1:
                schedule.run_pending()
                sleep(10)
        except KeyboardInterrupt:
            print("Daemon stopped")
    
    def full_sync(self):
        #TODO: make fetch a generator and stream the records to the DAL
        print(f"Full sync started")
        data = self.client.fetch()
        self.dal.push_to_db(data)
        print(f"Full sync done")
        sys.exit(0)

    def incremental_sync(self):
        end = arrow.utcnow()
        start = end.shift(hours=-1)
        print(f"Hourly sync started for {start}-{end} period")
        data = self.client.fetch(start, end)
        self.dal.push_to_db(data)
        return True


if __name__ == '__main__':
    app = DataSync()
    app.main()

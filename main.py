"""Simple command line interface for making Discourse API queries."""

from client import DiscourseClient

def main():
    """Runs the CLI application"""
    host = input("Enter the Discourse API host URL: ")
    api_username = input("Enter username: ")
    api_key = input("Input API key: ")
    while True:
        start_date = input("Input the start date in the year-month-day format (ex: 2022-05-30): ")
        end_date = input("Input the end date (input start date again if you only want one day): ")
        client = DiscourseClient(host, api_username, api_key, start_date, end_date)
        print(client.get_report())
        if input("Are you done? (True/False) ").lower() == "true":
            return

if __name__ == "__main__":
    main()

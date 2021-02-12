import argparse
import sys
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import yfinance as yf
import functools
print = functools.partial(print, flush=True)  # Prevent print statements from buffering till end of execution


def parse_and_validate_args():

    parser = argparse.ArgumentParser()
    parser.add_argument("ticker_list_filepath",
                            help="file system path to file containing the list of tickers")
    parser.add_argument("csv_output_filepath",
                            help="file system path for output CSV file")
    parser.add_argument("periods",
                            nargs='*',
                            help="numbers of days or years to find growth over, e.g.: 30 90 360 2y 5y")


    args = parser.parse_args()

    # Check if input file exists
    if not os.path.isfile(args.ticker_list_filepath):
        sys.exit(f"\nERROR: Ticker list file '{args.ticker_list_filepath}' does not exist.\n\nExiting.\n")

    if args.periods == []:
        print(f"\nERROR: 'periods' argument is required.\n")
        parser.print_help()
        sys.exit(f"\n\nExiting.\n")

    args.year_periods = []
    args.day_periods = []
    for period_idx, period in enumerate(args.periods):
        try:
            if period.endswith('y'):
                args.year_periods.append(int(period[:-1]))
            else:
                args.day_periods.append(int(period))
        except ValueError:
            sys.exit(f"\nERROR: period argument '{period}' is invalid. Only integer values are allowed.\n\nExiting.\n")

    return args


def get_dates(args):

    dates = []

    for num_days in args.day_periods:
        dates.append(datetime.today() - timedelta(days=num_days))
    for num_years in args.year_periods:
        dates.append(datetime.today() - relativedelta(years=num_years))

    dates = [date.strftime('%Y-%m-%d') for date in dates]  # Convert dates to strings
    dates.sort(reverse=True)  # Sort dates starting with most recent

    return dates


def open_and_read_tickers_file(args):

    file = open(args.ticker_list_filepath)
    tickers = file.readlines()
    file.close()

    tickers = [ticker.strip() for ticker in tickers]  # Remove any leading or trailing whitespace
    
    return tickers

def get_prices(dates, tickers):

    # yfinance's history() 'end' argument must be 1 day after the start to get just the data for the start date, so get those end dates
    end_dates = [(datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d') for date in dates]  # Subtract one day from each date

    current_label = datetime.now().strftime('%Y-%m-%d %H:%M')
    tmp_columns = ['ticker', current_label] + dates
    df = pd.DataFrame(columns=tmp_columns)

    for ticker in tickers:

        ticker_data = yf.Ticker(ticker)

        close_vals = []

        for date_idx, date in enumerate(dates):
            vals_on_date_df = ticker_data.history(period='1d', start=date, end=end_dates[date_idx], debug=False)
            
            try_attempts = 1
            new_date = date

            while (vals_on_date_df.empty and try_attempts < 3):  # Allow only 3 trys for Saturday, Sunday, and a market holiday
                
                timestamp = int(datetime.strptime(new_date, "%Y-%m-%d").timestamp())
                timestamp1 = timestamp - 60*60*24*3
                timestamp2 = timestamp + 60*60*24*3
                print(f"  No data found for {ticker} on {new_date}, see: https://finance.yahoo.com/quote/{ticker}/history?period1={timestamp1}&period2={timestamp2}. ", end="")

                new_end_date = new_date
                new_date = (datetime.strptime(new_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')  # Subtract one day
                print(f"Using {new_date} instead.")
                vals_on_date_df = ticker_data.history(period='1d', start=new_date, end=new_end_date, debug=False)
                try_attempts += 1

            if not vals_on_date_df.empty:
                close_vals.append(vals_on_date_df['Close'].iloc[0])
                dates[date_idx] = new_date  # This will change the date for all tickers after this one
            else:
                close_vals.append(np.NaN)

        current_price = ticker_data.info['ask']
        df.loc[len(df)] = [ticker, current_price] + close_vals
        
    # Some different dates may have been used, so fix column labels
    date_column_labels = [date + '_close' for date in dates]
    df.columns = ['ticker', current_label] + date_column_labels

    return df


def calculate_returns(df):

    

    return df


def dfs_to_csv(df, output_file_path, verbose=False):

    df.to_csv(output_file_path, index=False, float_format="%.3f")

    if verbose:
        print(f"Data written to '{output_file_path}'.")



def main():

    args = parse_and_validate_args()

    dates = get_dates(args)

    print("\nReading in data... ", end="")
    tickers = open_and_read_tickers_file(args)
    print("done.")
    
    print("\nFetching and processing data...")
    df = get_prices(dates, tickers)
    df = calculate_returns(df)
    print("done.")

    print(df)
    
    print("\nWriting data to CSV file... ", end="")
    dfs_to_csv(df, args.csv_output_filepath, verbose=True)
    print("done.")

    print("\nAll done. Exiting.\n\n")


if __name__ == '__main__':

    main()

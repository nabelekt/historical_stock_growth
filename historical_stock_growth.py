MARKET_FOR_HOLIDAYS = 'NASDAQ'


import argparse
import sys
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import yfinance as yf
import pandas_market_calendars as mcal
import functools
print = functools.partial(print, flush=True)  # Prevent print statements from buffering till end of execution


def parse_and_validate_args():

    parser = argparse.ArgumentParser()
    parser.add_argument("ticker_list_filepath",
                            help="file system path to file containing the list of tickers")
    parser.add_argument("csv_output_filepath",
                            help="file system path for output CSV file")
    parser.add_argument("include_close_prices",
                            choices=['0', '1'],
                            help="boolean flag indicating if close prices for each date should be included in output")
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

    args.include_close_prices = int(args.include_close_prices)

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

    current_label = datetime.now().strftime('%Y-%m-%d %H:%M') + ' price'
    tmp_columns = ['ticker', current_label] + dates
    df = pd.DataFrame(columns=tmp_columns)

    for ticker_idx, ticker in enumerate(tickers):

        print(f"\n  Getting data for {ticker} ({ticker_idx+1}/{len(tickers)})...")
        ticker_data = yf.Ticker(ticker)

        close_vals = []

        for date_idx, date in enumerate(dates):
            vals_on_date_df = ticker_data.history(period='1d', start=date, end=end_dates[date_idx], debug=False)
            
            if not vals_on_date_df.empty:
                close_vals.append(vals_on_date_df['Close'].iloc[0])
            else:
                timestamp = int(datetime.strptime(date, "%Y-%m-%d").timestamp())
                timestamp1 = timestamp - 60*60*24*3  # 3 days before
                timestamp2 = timestamp + 60*60*24*3  # 3 days ahead
                print(f"  No data found for {ticker} on {date}, see: https://finance.yahoo.com/quote/{ticker}/history?period1={timestamp1}&period2={timestamp2}.")
                
                close_vals.append(np.NaN)

        current_price = ticker_data.info['regularMarketPrice']
        # current_price = ticker_data.info['ask']
        df.loc[len(df)] = [ticker, current_price] + close_vals
        
    # Some different dates may have been used, so fix column labels
    date_column_labels = [date + ' close' for date in dates]
    df.columns = ['ticker', current_label] + date_column_labels

    return df


# If the specified date was a weekend day or market holiday, use the next previous market day instead
def check_dates(dates):

    market_holidays = mcal.get_calendar(MARKET_FOR_HOLIDAYS).holidays().holidays
    market_holidays = [pd.to_datetime(holiday).strftime('%Y-%m-%d') for holiday in market_holidays]
    
    for date_idx, date in enumerate(dates):

        while((date in market_holidays) or (datetime.strptime(date, "%Y-%m-%d").weekday() > 4)):  # weekday() returns 0-4 for Monday-Friday
            print(f"  {date} was on a weekend or was a market holiday. Using ", end="")
            date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')  # Subtract one day
            print(f"{date}.")

        dates[date_idx] = date

    return dates


def calculate_returns(df):

    # Get dataframe of return values
    returns_df = df.apply(calculate_return_row, axis=1)
    
    # Append returns df to close prices df
    df = pd.concat([df, returns_df], axis=1).sort_index(axis=1, ascending=False)
    
    # Rearrange columns so that close price precedes each respective return value
    return_cols = df.columns[2::2]
    close_cols  = df.columns[3::2]
    reordered_cols = list(df.columns[0:2])
    reordered_cols = reordered_cols + [col for idx, _ in enumerate(return_cols) for col in [close_cols[idx], return_cols[idx]]]
    df = df[reordered_cols]

    return df


def calculate_return_row(row: pd.Series):

    current_price = row[1]
    close_prices  = row[2:]
    
    returns = [calculate_return(current_price, close_price) for close_price in close_prices]
    index = [label.replace('close', 'return') for label in row.index[2:]]
    returns = pd.Series(returns, index=index)

    return returns


def calculate_return(current_val, initial_val):

    return (current_val - initial_val) / initial_val * 100


def append_period_headers(df):

    # print(df)

    now = datetime.now()
    dates = [datetime.strptime(col_name[0:10], '%Y-%m-%d') for col_name in list(df.columns)[1:]]

    num_days_list = [(now - date).days for date in dates]

    period_headers = ['', '']  # '' and '' for ticker column and for current price column

    for num_days in num_days_list:
        if num_days >= 365:
            num_years = round(num_days/365.25, 1)  # Round to 1 decimal place; .25 to account for leap years
            period_headers.append(f"{num_years} years")
        else:
            period_headers.append(f"{num_days} days")
    
    period_df = pd.DataFrame([period_headers])
    # print(period_df)
    # df = pd.concat([df.iloc[:1], period_df, df.iloc[1:]])#.reset_index(drop=True)

    # sys.exit()
    
    return df


# Drop close prices from df
def drop_close_prices(args, df):
    
    if not args.include_close_prices:
        cols = df.columns
        cols_to_keep = [col for col in cols if not col.endswith('close')]
        df = df[cols_to_keep]

    return df


def dfs_to_csv(df, output_file_path, verbose=False):

    df.to_csv(output_file_path, index=False)#, float_format="%.3f")

    if verbose:
        print(f" Data written to '{output_file_path}'.")


def main():

    args = parse_and_validate_args()

    dates = get_dates(args)

    print("\nReading in data... ", end="")
    tickers = open_and_read_tickers_file(args)
    print("done.")
    
    print("\nFetching and processing data...")
    dates = check_dates(dates)
    df = get_prices(dates, tickers)
    df = calculate_returns(df)
    df = append_period_headers(df)
    df = drop_close_prices(args, df)
    print("done.")
    
    print("\nWriting data to CSV file...")
    dfs_to_csv(df, args.csv_output_filepath, verbose=True)
    print("done.")

    print("\nAll done. Exiting.\n\n")


if __name__ == '__main__':

    main()

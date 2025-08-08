import pandas as pd
import datetime
import glob
import os


def calculate_fifo_profit(buys, sell_quantity, sell_price, market, sell_date):
    profit = 0
    remaining_sell_quantity = sell_quantity
    trades = []

    for i, buy in enumerate(buys):
        buy_quantity = buy['quantity']
        buy_price = buy['price']
        buy_date = buy['date']

        if buy_quantity <= remaining_sell_quantity:
            trade_profit = buy_quantity * (sell_price - buy_price)
            profit += trade_profit
            trades.append({
                'date of acquisition': buy_date,
                'date of transfer': sell_date,
                'cost of acquisition': buy_quantity * buy_price,
                'consideration received': buy_quantity * sell_price,
                'income': trade_profit,
                'coin name (market)': market
            })
            remaining_sell_quantity -= buy_quantity
            buys[i]['quantity'] = 0  # Mark as fully consumed
        else:
            trade_profit = remaining_sell_quantity * (sell_price - buy_price)
            profit += trade_profit
            trades.append({
                'date of acquisition': buy_date,
                'date of transfer': sell_date,
                'cost of acquisition': remaining_sell_quantity * buy_price,
                'consideration received': remaining_sell_quantity * sell_price,
                'income': trade_profit,
                'coin name (market)': market
            })
            buys[i]['quantity'] -= remaining_sell_quantity
            remaining_sell_quantity = 0
            break

    # Remove fully consumed buy orders
    buys[:] = [buy for buy in buys if buy['quantity'] > 0]

    return profit, trades, buys


def calculate_pnl(data_dir='cleaned_data', financial_year_start='2024-04-01', financial_year_end='2025-03-31'):
    # Find all trade files
    exchange_files = glob.glob(os.path.join(data_dir, 'Exchange_Trades_*.csv'))
    p2p_files = glob.glob(os.path.join(data_dir, 'P2P_Trades_*.csv'))
    all_files = exchange_files + p2p_files

    # Read and combine all trades
    all_trades = []
    for file in all_files:
        df = pd.read_csv(file)
        # Extract year from filename and add as a column
        year = file.split('_')[-1].split('.')[0]
        df['Year'] = year
        all_trades.append(df)

    all_trades = pd.concat(all_trades, ignore_index=True)

    # Convert 'Date' to datetime objects
    all_trades['Date'] = pd.to_datetime(all_trades['Date'])

    # Sort all trades by date
    all_trades = all_trades.sort_values(by='Date')

    # Financial year start and end dates
    fy_start = pd.to_datetime(financial_year_start)
    fy_end = pd.to_datetime(financial_year_end)

    # FIFO Implementation
    holdings = {}
    pl_trades = []

    # Process all trades to build the holdings ledger
    for index, trade in all_trades.iterrows():
        date = trade['Date']
        market = trade['Market']
        trade_type = trade['Trade Type']
        price = trade['Price']
        volume = trade['Volume']
        global sell_date  # make sell_date accessible in calculate_fifo_profit
        sell_date = date

        if market not in holdings:
            holdings[market] = []

        if trade_type == 'Buy':
            holdings[market].append(
                {'date': date, 'quantity': volume, 'price': price})

        elif trade_type == 'Sell':
            sell_quantity = volume
            # Only calculate profit if the trade is within the financial year
            if fy_start <= date <= fy_end:
                profit, trades, holdings[market] = calculate_fifo_profit(
                    holdings[market], sell_quantity, price, market, sell_date)
                pl_trades.extend(trades)
            else:
                # Still need to consume from buys if outside the financial year
                profit, trades, holdings[market] = calculate_fifo_profit(
                    holdings[market], sell_quantity, price, market, sell_date)

    pl_df = pd.DataFrame(pl_trades)
    if not pl_df.empty:
        pl_df.to_csv('pnl_statement.csv', index=False)

    return pl_df


if __name__ == '__main__':
    pnl_result = calculate_pnl()
    print(pnl_result)


def read_excel_sample(file_path, num_rows=5):
    try:
        df_sample = pd.read_excel(file_path, nrows=num_rows)
        print(f"Sample data from {file_path}:/n", df_sample.head())
        return df_sample
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

import os
import pandas as pd


def ensure_directory_exists(directory):
    """Ensure the specified directory exists; create it if it doesn't."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def export_sheets_to_csv(excel_dir="data", output_dir="cleaned_data"):
    """Export specified sheets from Excel files to CSV files, maintaining financial year in filenames."""
    ensure_directory_exists(output_dir)

    for file_name in os.listdir(excel_dir):
        if file_name.endswith(".xlsx"):
            input_file_path = os.path.join(excel_dir, file_name)
            try:
                xls = pd.ExcelFile(input_file_path)
                sheet_names = xls.sheet_names
                print(f"Processing {file_name} with sheets: {sheet_names}")

                cleaned_name = file_name.replace(".xlsx", ".csv")
                cleaned_name = cleaned_name.replace("WazirX_TradeReport_", "")
                cleaned_name = cleaned_name.replace("-04-01", "")
                cleaned_name = cleaned_name.replace("-03-31", "")
                

                if "Exchange Trades" in sheet_names:
                    exchange_trades_df = pd.read_excel(
                        xls, sheet_name="Exchange Trades")
                    output_file = os.path.join(
                        output_dir, f"Exchange_Trades_{cleaned_name}")
                    exchange_trades_df.to_csv(output_file, index=False)
                    print(f"Exported Exchange Trades to {output_file}")

                # Updated to check for "P2P Trades" with capital "T"
                if "P2P Trades" in sheet_names:
                    print(f"Found \"P2P Trades\" sheet in {file_name}.")
                    p2p_trades_df = pd.read_excel(xls, sheet_name="P2P Trades")
                    output_file = os.path.join(
                        output_dir, f"P2P_Trades_{cleaned_name}")
                    p2p_trades_df.to_csv(output_file, index=False)
                    print(f"Exported P2P Trades to {output_file}")
                else:
                    print(f"No \"P2P Trades\" sheet found in {file_name}")

            except Exception as e:
                print(f"Error processing {file_name}: {e}")


# Execute the function
if __name__ == "__main__":
    export_sheets_to_csv()

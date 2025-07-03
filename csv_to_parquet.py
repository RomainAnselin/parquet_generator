import pandas as pd

def convert_csv_to_parquet():
    # Read the CSV file
    print("Reading CSV file...")
    df = pd.read_csv('output.csv')
    
    # Ensure the date column is properly parsed as datetime
    # Assuming the date column is named 'date' - adjust if different
    # df['date'] = pd.to_datetime(df['date'])
    
    # Write to parquet format
    print("Converting to Parquet format...")
    output_file = 'output.parquet'
    df.to_parquet(output_file, index=False)
    print(f"Conversion complete. Parquet file saved as: {output_file}")

if __name__ == "__main__":
    convert_csv_to_parquet() 
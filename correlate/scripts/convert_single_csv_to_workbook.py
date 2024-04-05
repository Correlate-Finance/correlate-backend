import pandas as pd


def convert_csv_to_workbook(filename: str):
    """
    Convert a single CSV file to an Excel workbook with a single sheet.
    """
    # Load the Excel file
    df = pd.read_csv(filename, sep=",", header=3, usecols=[0, 1, 2, 3, 4, 5])

    # List of metric columns
    metric_columns = [
        "World Share",
        "RPK",
        "ASK",
        "PLF (%-pt)",
    ]  # Replace with your actual metric column names
    print(df)
    # Create a new Excel writer object
    with pd.ExcelWriter(
        "output_excel_file.xlsx", engine="openpyxl", mode="w"
    ) as writer:
        for metric in metric_columns:
            for name in df["Name"].unique():
                print(name, metric)
                # Filter data for the current Name-Metric combination
                data = df[["Date", metric]][df["Name"] == name]
                data.columns = ["Date", "Value"]  # Rename columns

                # # Create a new DataFrame for the output
                output_df = pd.DataFrame(
                    [["", ""] for x in range(len(data.values) + 6)],
                )

                output_df.loc[2] = [
                    "Source",
                    "IATA",
                ]  # Insert Source and IATA at the 4th row
                output_df.loc[5] = [
                    "Date",
                    "Value",
                ]
                output_df.iloc[6:] = (
                    data.values
                )  # Insert the data starting from the 5th row

                # Write to a new sheet in the Excel file
                sheet_name = f"{name} - {metric}"
                # print(output_df)
                output_df.to_excel(writer, sheet_name=sheet_name, index=False)

    print("Excel file processing complete.")


convert_csv_to_workbook(
    "/Users/vidushijain/Downloads/IATA Data - Air Passenger Market - IATA - Air Passenger Data.csv"
)

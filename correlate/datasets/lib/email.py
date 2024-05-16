from datetime import datetime, timedelta


def create_new_data_report_email(
    added_records: list[tuple[str, float]],
    updated_records: list[tuple[str, datetime, float, float]],
    source,
    total_time: timedelta,
):
    # Create a report email with the added and updated records

    email_head = """<head>
                    <style>
                        body {
                            font-family: Arial, sans-serif;
                            line-height: 1.6;
                            background-color: #f4f4f4;
                            padding: 20px;
                        }
                        .container {
                            background-color: #ffffff;
                            padding: 20px;
                            border-radius: 8px;
                            box-shadow: 0 0 10px rgba(0,0,0,0.1);
                        }
                        h2 {
                            color: #333;
                        }
                        table {
                            width: 100%;
                            border-collapse: collapse;
                            margin-top: 20px;
                        }
                        th, td {
                            border: 1px solid #ddd;
                            padding: 8px;
                            text-align: left;
                        }
                        th {
                            background-color: #f8f8f8;
                        }
                    </style>
                </head>"""

    added_records_rows = ""
    for series_id, count in added_records:
        added_records_rows += f"<tr><td>{series_id}</td><td>{count}</td></tr>"

    updated_records_rows = ""
    for series_id, date, new_value, old_value in updated_records:
        updated_records_rows += f"<tr><td>{series_id}</td><td>{date}</td><td>{old_value}</td><td>{new_value}</td></tr>"

    email_html = f"""
                <!DOCTYPE html>
                <html>
                {email_head}
                <body>
                    <div class="container">
                        <h2>Data Update Report</h2>

                        <p>Time taken to fetch and update data: {total_time}</p>
                        <p>Here is the latest update on the datasets for {source} we are monitoring:</p>

                        <h3>Overview</h3>
                        <p>Datasets with new data: {len(added_records)} n</p>
                        <p>Total data points added: {sum([count for _, count in added_records])}</p>

                        <p>Datasets with updated data: {len(set([series_id for series_id, _, _, _ in updated_records]))} n</p>
                        <p>Total data points updated: {len(updated_records)}</p>

                        <h3>New Data added</h3>
                        <table>
                            <tr>
                                <th>Dataset</th>
                                <th>Record Count</th>
                            </tr>
                            {added_records_rows}
                        </table>

                        <h3>Updated Data</h3>
                        <table>
                            <tr>
                                <th>Dataset</th>
                                <th>Updated Date</th>
                                <th>Old Value</th>
                                <th>New Value</th>
                            </tr>
                            {updated_records_rows}
                        </table>
                    </div>
                </body>
                </html>
    """

    return email_html

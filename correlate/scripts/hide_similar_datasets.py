import openpyxl

"""
CSV file format:
| internal_name | external_name | [similar external names] | [similar internal names]
"""


def extract_rows_from_csv(excel_file: str) -> list[tuple[str, list[str]]]:
    rows = []
    workbook = openpyxl.load_workbook(filename=excel_file, data_only=True)
    for sheet in workbook:
        for row in sheet.iter_rows():
            try:
                internal_name = row[0].value
                similar_internal_names = row[2].value
                similar_names = similar_internal_names.strip("[]").split(",")
                rows.append((internal_name, similar_names))
            except:
                print(row)

    return rows


def hide_similar_datasets(csv_file: str):
    rows = extract_rows_from_csv(csv_file)
    datasets_to_hide = set([])
    master_datasets = {}

    for internal_name, similar_internal_names in rows:
        # We are already hiding this dataset we shouldnt look at the ones that are similar to it
        if internal_name in datasets_to_hide:
            continue

        master_datasets[internal_name] = []
        for similar_internal_name in similar_internal_names:
            if similar_internal_name in datasets_to_hide or master_datasets:
                continue
            master_datasets[internal_name].append(similar_internal_name)

    return master_datasets


def pretty_print_dict(dictionary: dict):
    for key, value in dictionary.items():
        print(f"{key}: {value}")

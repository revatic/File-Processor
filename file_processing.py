import os
import pandas as pd

def process_files(input_folder, output_folder):
    try:
        #getting files which are having .dat
        input_files = [file for file in os.listdir(input_folder) if file.endswith('.dat')]

        individual_file_counts = {}
        total_duplicates = 0

        merged_data = pd.DataFrame()

        for file in input_files:
            file_path = os.path.join(input_folder, file)
            chunks = pd.read_csv(file_path, sep='\t', chunksize=1000)
            count = 0
            for chunk in chunks:
                count += len(chunk)
                merged_data = pd.concat([merged_data, chunk])

            individual_file_counts[file] = count

            # Checking duplicate values
            duplicates = merged_data.duplicated().sum()
            total_duplicates += duplicates

        merged_data = merged_data.drop_duplicates()#checking duplicates

        null_values_present = merged_data.isnull().values.any()#checking null values
        #for second highest salary
        sorted_data = merged_data.sort_values(by='basic_salary', ascending=False)
        second_highest_salary = sorted_data['basic_salary'].iloc[1]

        #average salary
        average_salary = round(sorted_data['basic_salary'].mean(),1)
        #last row
        footer_row = pd.DataFrame({"id": [f"Second Highest Salary={second_highest_salary}"],
                                   "first_name": [f"Average Salary={average_salary}"]})

        sorted_data = pd.concat([sorted_data, footer_row], ignore_index=True)
        #to csv file
        output_file = os.path.join(output_folder, "result.csv")
        sorted_data.to_csv(output_file, index=False)

        print("Individual File Counts:")
        for file, count in individual_file_counts.items():
            print(f"{file}: {count}")

        print("\nTotal Result Data Count:", len(sorted_data) - 1)  # Exclude footer row
        print("Total Duplicates:", total_duplicates)
        print("Null Values Present:", null_values_present)

    except Exception as e:
        print("An error occurred:", e)


input_folder = "source_folder"
output_folder = "destination_folder"
process_files(input_folder, output_folder)

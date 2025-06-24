# *********************************************  Process ColdSpray data and store CSV files in MongoDB ************************************************

import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

def process_ColdSpray_data_folders(input_folder):
    """
    Process data folders and store CSV files in MongoDB
    
    Args:
        input_folder (str): Path to the input folder containing date-named subfolders
    """
    # MongoDB connection setup
    try:
        # Replace with your MongoDB connection string if different
        client = MongoClient('mongodb+srv://root:root@remanetdashboard.evoss.mongodb.net/?retryWrites=true&w=majority&appName=RemanetDashboard')
        db = client['DataIngestion']
        print("Connected to MongoDB")
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return

    # Iterate through date folders
    for date_folder in os.listdir(input_folder):
        date_folder_path = os.path.join(input_folder, date_folder)
        
        # Validate it's a folder and matches date format
        if os.path.isdir(date_folder_path):
            try:
                # Validate date format
                date_folder = datetime.strptime(date_folder, '%Y-%m-%d').date()
                print(f"Processing folder: {date_folder}")
            except ValueError:
                print(f"Skipping invalid folder name: {date_folder}")
                continue

            # Process coldspray and kuka folders
            for machine_type in ['coldspray']:
                machine_folder = os.path.join(date_folder_path, machine_type)
                
                # Check if machine folder exists
                if not os.path.exists(machine_folder):
                    print(f"Folder not found: {machine_folder}")
                    continue

                # Process CSV files in the machine folder
                for csv_file in os.listdir(machine_folder):
                    if csv_file.endswith('.csv'):
                        csv_path = os.path.join(machine_folder, csv_file)
                        
                        try:
                            # Read CSV file
                            if machine_type == 'kuka':
                                df = pd.read_csv(csv_path)
                            else:
                                Coldspray = pd.read_csv(csv_path,on_bad_lines='skip',sep=';')

                                Coldspray["Time"] = pd.to_datetime(Coldspray["Time"], format='%H:%M:%S').dt.time

                                Coldspray["DateTime"] = Coldspray["Time"].apply(
                                    lambda x: pd.Timestamp.combine(date_folder, x)
                                )

                                Coldspray["Time"] = Coldspray["DateTime"]
                                Coldspray.drop("DateTime", axis=1, inplace=True)

                                Coldspray_KPIs = Coldspray.iloc[:,1:6].drop("Q_PG_He",axis=1)

                                for col in Coldspray_KPIs.columns:
                                  if Coldspray_KPIs[col].dtype == "object":
                                    Coldspray_KPIs[col] = Coldspray_KPIs[col].str.replace(',', '.')
                                    Coldspray_KPIs[col] = Coldspray_KPIs[col].astype(float)
                            
                            # Convert to dictionary for MongoDB storage
                            records = Coldspray_KPIs.to_dict('records')
                            
                            # Insert into appropriate collection
                            collection = db[machine_type]
                            
                            # Insert records
                            if records:
                                collection.insert_many(records)
                                print(f"Inserted {len(records)} records from {csv_file} into {machine_type} collection")
                        
                        except Exception as e:
                            print(f"Error processing {csv_file}: {e}")

    # Close MongoDB connection
    client.close()

# *********************************************  Process Mic data and store CSV files in MongoDB ************************************************

import os
import datetime
from  pymongo import MongoClient
import pandas as pd
from bson.binary import Binary

def process_micro_data(directory_path):
    """
    Process binary files and store them in MongoDB
    
    Args:
        directory_path (str): Path to the directory containing binary files
    """
    # Connect to MongoDB
    client = MongoClient('mongodb+srv://root:root@remanetdashboard.evoss.mongodb.net/?retryWrites=true&w=majority&appName=RemanetDashboard')
    db = client["DataIngestion"]
    collection = db["micro_1"]

    # Process each bin file in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".bin"):
            # Parse Unix timestamp from the filename
            try:
                unix_timestamp = int(os.path.splitext(filename)[0])
                
                # Convert Unix timestamp to datetime object
                date_time = datetime.datetime.fromtimestamp(unix_timestamp)
                
                # Format the datetime as a string (if needed for display)
                formatted_date = date_time.strftime('%Y-%m-%d %H:%M:%S')
                
                # Read binary data from the file
                file_path = os.path.join(directory_path, filename)
                with open(file_path, "rb") as bin_file:
                    binary_data = bin_file.read()
                
                # Create document to insert into MongoDB
                document = {
                    "timestamp": date_time,  # Store as actual datetime object for querying
                    "data": Binary(binary_data)  # Store binary data
                }
                
                # Insert document into collection
                collection.insert_one(document)
                
                print(f"Processed file: {filename}, Date: {formatted_date}")
                
            except ValueError:
                print(f"Skipping file {filename}: not a valid unix timestamp")
            except Exception as e:
                print(f"Error processing file {filename}: {str(e)}")

    print("All files processed and stored in MongoDB")
    # Close MongoDB connection
    client.close()





def main():

    # input_folder = '/content/Coldspray-extract/'
    # process_ColdSpray_data_folders(input_folder)


    micro_directory_path = "micro_x/micro_1/"
    process_micro_data(micro_directory_path)

if __name__ == '__main__':
    main()




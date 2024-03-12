import time
import json
import random
import pyodbc
from datetime import datetime, date
from uuid import uuid4
import unicodedata
import re
from slugify import slugify  
# Function to connect to the database
def connect_to_database(server, user, password, database):
    try:
        conn_str = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={user};PWD={password}"
        conn = pyodbc.connect(conn_str)
        print("Connected to database successfully!")
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        return None

# Function to execute a query and fetch results
def execute_query(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor
    except Exception as e:
        print("Error executing query:", e)
        return None

def convert_to_slug(input_string):
    normalized_string = input_string.lower()
    normalized_string = re.sub(r"[^\w\s-]", "", normalized_string)

    normalized_string = re.sub(r"\s+", "-", normalized_string)

    normalized_string = re.sub(r"", "", normalized_string)
    normalized_string = slugify(normalized_string)

    return normalized_string

def remove_new_line(input_string):
    return input_string.replace("/n", " ").replace("https://taigame.org/misc/helpPlay#system-requirements", "/system-requirements")


# Function to convert objects to JSON serializable format
def convert_to_serializable(obj):
    if isinstance(obj, (datetime, date)):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    else:
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def main():
    # Enter your database connection details
    server = '26.171.26.21'
    database = 'webgame'
    user = 'TaiKhoanAdminChung'
    password = 'admin'
    query = "SELECT * FROM Game ORDER BY yearRelease"

    # Keys to include in the JSON output
    keys_to_include = ['attGame', 'dateModified', 'desGame', 'devsGame', 'enableGame', 'id', 'imgName', 'listImage', 'nameGame', 'numSale', 'priceGame', 'publisher', 'requireGame', 'tagGame', 'typeGame', 'yearRelease']

    # Connect to the database
    conn = connect_to_database(server, user, password, database)
    
    if conn:
        # Execute the query
        cursor = execute_query(conn, query)
        if cursor:
            batch_count = 0
            game_count = 0
            
            gameList = []

            for row in cursor:
                row_dict = {}
                for i, column in enumerate(cursor.description):
                    if column[0] in keys_to_include:
                        if isinstance(row[i], datetime):
                            row_dict[column[0]] = row[i].strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            if column[0] == 'nameGame':
                                    row_dict[column[0]] = row[i]
                                    row_dict['slug'] =   convert_to_slug(row[i])    
                            elif column[0] == 'desGame' or column[0] == 'requireGame' or column[0] == 'attGame':  
                                row_dict[column[0]] = remove_new_line(row[i])         
                            elif column[0] == 'numSale':
                                row_dict[column[0]] = random.randint(100, 11199)
                            elif column[0] == 'priceGame':
                                row_dict[column[0]] =round( float(row[i]+random.random()),2)
                                if random.random() < 0.7:  # 50% chance of discount
                                    discount_percent = random.randint(1, 99)
                                    row_dict['discountPercent'] = discount_percent  # Discount from 10% to 99%
                                    row_dict['priceGameNow'] = round(row[i] * (1 - discount_percent / 100),2)
                                    row_dict['isDiscount'] = True   
                                else:                        
                                    row_dict['priceGameNow'] = row[i]
                                    row_dict['discountPercent'] = 0
                                    row_dict['isDiscount'] = False
                                
                            elif column[0] == 'enableGame':
                                row_dict[column[0]] = True
                            elif column[0] == 'id':
                                row_dict[column[0]] = game_count
                            elif column[0] in ['typeGame', 'tagGame', 'listImage', 'devsGame', 'publisher']:
                                try:
                                    arr = eval(row[i])  # Be careful with eval(), make sure the data is safe
                                    row_dict[column[0]] = arr
                                except (SyntaxError, TypeError):
                                    row_dict[column[0]] = row[i]  # If conversion fails, use the original value
                            else:
                                row_dict[column[0]] = row[i]                                                               
                attribute = []   
                for item in row_dict['typeGame']:
                    attribute.append({
                        'k': 'category',
                        'v': convert_to_slug(item),
                        'u': item
                    }
                )
                for item in row_dict['tagGame']:
                    attribute.append({
                        'k': 'tag',
                        'v': convert_to_slug(item),
                        'u': item
                    }
                )
                for item in row_dict['devsGame']:
                    attribute.append({
                        'k': 'developer',
                        'v': convert_to_slug(item),
                        'u': item
                    }
                )
                for item in row_dict['publisher']:
                    attribute.append({
                        'k': 'publisher',
                        'v': convert_to_slug(item),
                        'u': item
                    }
                )
                attribute.append({
                        'k': 'yearRelease',
                        'v': row_dict['yearRelease']
                }) 
                row_dict['attribute'] = attribute
                gameList.append(row_dict)  

                game_count += 1
                print('this is game index ',game_count)
                
            with open(f'data.json', 'w') as json_file:
                json.dump(gameList, json_file, indent=4, default=convert_to_serializable)
            print(f"JSON data for batch {batch_count} has been saved to data_batch_{batch_count}.json")
            game_count = 0
        conn.close()
    else:
        print("Unable to connect to the database.")

# def read_all_data_bactch_append_to_datadotjson():
#     result = []
#     for i in range(1, 28):
#         with open(f'data_batch_{i}.json') as f:
#             json_data = json.load(f)
#             result.append(json_data)

#     with open('data.json', 'w') as f:
#         json.dump(result, f, indent=4, default=convert_to_serializable)
#     print("All data has been saved to data.json")

if __name__ == "__main__":
    main()   
import pandas as pd
import sqlite3
import json
import time

INPUT_FILE_NAME = 'CSA upload sheet.xlsx'
DB_NAME = 'tasks.db'

input_df = pd.read_excel(INPUT_FILE_NAME, skiprows=1)
DROP_FILED_LIST = ["通訊人姓名", "通訊人id"]
# drop the first column
input_df = input_df.drop(input_df.columns[0], axis=1)
input_df = input_df.drop(DROP_FILED_LIST, axis=1)
# replace nan with ""
input_df = input_df.fillna("")
# print(input_df.head(20))

def create_json(row):
    json_str = "["
    for i, column in enumerate(input_df.columns):

        if column == "通訊人":
            if row[column] == "":
                json_str += '""'
            else:
                c_name_chn, c_personid = row[column].split("=")
                json_str += '{"c_name_chn":"' + c_name_chn + '","c_personid":"' + c_personid + '"}'
        else:
            json_str += '"' + str(row[column]) + '"'
        if i != len(input_df.columns) - 1:
            json_str += ","
    json_str += "]"
    # json_output = {row["人物id"]: json.loads(json_str)}
    return json.dumps(json.loads(json_str))

new_data_json= input_df.apply(create_json, axis=1)
print(new_data_json[0])
print(new_data_json[9])

# read the json from sqlite database, go to tasks table read data column as json
conn = sqlite3.connect(DB_NAME)
c = conn.cursor()
c.execute('SELECT data FROM tasks')
data = c.fetchall()
# convert the data to json
json_data = json.loads(data[0][0])
print(json_data.keys())
# add new_data_json_df to json_data's "data" key
for i, row in input_df.iterrows():
    # update method is that the key in the row should be used to create the key in json_data["data"], the value in the row should be used to create the value in json_data["data"]
    json_data["data"][row["行號"]] = json.loads(new_data_json[i])

# change the title by Ming Letters yyyymmdd, yyymmdd is the current date
json_data["title"] = "Ming Letters " + time.strftime("%Y%m%d")
print(json_data["title"])

# write the json_data to output.json, encode the json_data to utf-8
with open('output.json', 'w', encoding='utf-8') as f:
    json.dump(json_data, f, ensure_ascii=False)

import shutil
shutil.copyfile(DB_NAME, "_" + DB_NAME)

# update the tasks table in the .db file
c.execute('UPDATE tasks SET data = ?', (json.dumps(json_data),))
conn.commit()
conn.close()

print("Update successfully!")
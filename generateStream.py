import pandas as pd
import csv
import os
import uuid
import time

df = pd.read_csv("source.csv")
# drop unneccesary columns
df = df.drop(columns=["Amount", "Class"])
# write to file
write_to_file = 'streaming_sensor_data.csv'
# remove the file if it exists - so its not always appending
try:
    os.remove(write_to_file)
except OSError:
    pass

df["Time"] = df["Time"].astype("str")

col_list = [i for i in df.columns if 'V' in i ]
for col in col_list:
    df[col] = df[col].astype('float')


while True:
    with open(write_to_file, 'a', newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        sample_frame = df.sample(n=5)
        # sample_frame['i'] = "i"
        # sample_frame['n'] = "n"
        sample_frame["uuid"] = [uuid.uuid4() for i in range(len(sample_frame))]

        # sample_cols = sample_frame.columns.tolist()
        # sample_cols = sample_cols[-3:] + sample_cols[:-3]
        # sample_frame = sample_frame[sample_cols]
        writer_list = sample_frame.values.tolist()
        writer.writerows(writer_list)
        print(writer_list)
        time.sleep(.20)
import csv
import os
import sys

def split_csv(sfp, dest, prefix, size_chunk):
    if size_chunk <= 0:
        return
        
    with open(sfp, 'r') as s:
        reader = csv.reader(s)
        headers = next(reader)
        fn = 0
        exist = True

        while exist:
            i = 0
            t_filename = f'{prefix}_{fn}.csv'
            t_filepath = os.path.join(dest, t_filename)
            print(t_filepath)
            with open(t_filepath, 'w', newline='') as target:                
                writer = csv.writer(target)
                while i < size_chunk:
                    if i == 0:
                        writer.writerow(headers)
                    try:
                        writer.writerow(next(reader))
                        i += 1
                    except:
                        exist = False
                        break
                return
            if i == 0:
                os.remove(t_filepath)
            fn += 1
  
split_csv('Iowa_Liquor_Sales.csv', 'chunks/', 'Iowa_Liquor_Sales_', 1000)
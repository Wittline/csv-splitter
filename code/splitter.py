import os
import gzip
import mmap
import concurrent.futures

class CSVSplit:

    def __init__(self, filepath, prefix, folder, header = True, sep = ','):
        self.filepath = filepath
        self.header = header
        self.prefix = prefix
        self.folder = folder
        self.sep = sep


    def __estimate_csv_rows(self, filename, header = True):

        count_rows = 0

        with open(filename, mode="r", encoding = "ISO-8859-1") as file_obj:

            with mmap.mmap(file_obj.fileno(), length=0, access=mmap.ACCESS_READ) as map_file:

                buffer = map_file.read(1<<13)
                file_size = os.path.getsize(filename)
                count_rows = file_size // (len(buffer) // buffer.count(b'\n')) - (1 if header else 0) 
        
        return count_rows        

    def __split__columns_process(self, filepath, column_name, chunk_size = 10):
        
        partitions = {}
        row_number = 0        

        with open(filepath) as f:

            if self.header:
                header = next(f)
                _header =  header.strip().split(self.sep)

            index_column = _header.index(column_name)
            del _header[index_column]
            _header = self.sep.join(_header) + '\n'

            for line in f:
                row = line.strip().split(self.sep)
                value = row[index_column]
                del row[index_column]
                if value not in partitions:
                    if self.header:
                        partitions[value] = [_header]
                    else:
                        partitions[value] = []

                partitions[value].append(self.sep.join(row) + '\n')
                row_number += 1
                
                if row_number == chunk_size:
                    yield partitions
                    row_number = 0
                    partitions = {}                 

        if row_number > 0:
            yield partitions


    def __split_process(self, filepath, chunk_size = 10):
        chunk = []
        with open(filepath) as f:

            if self.header:
                header = next(f)
                chunk.append(header)

            for line in f:
                chunk.append(line)
                if len(chunk) == chunk_size:
                    yield chunk

                    if self.header:
                        chunk = [header]
                    else:
                        chunk = []
        if chunk:
            yield chunk            
    
    def split_chunks_size_n(self, chunk_size = 10, compress = False):

        chunk_number = 0              
        for chunk in self.__split_process(self.filepath, chunk_size):

            format = 'csv'

            if compress:
                format = 'csv.gz'

            t_filename = f'{self.prefix}_{chunk_number}.{format}'
            t_filepath = os.path.join(self.folder, t_filename)

            if compress:
                chunk_bytes = gzip.compress(''.join(chunk).encode()[6:])
                with open(t_filepath, 'wb') as chunk_file:
                    chunk_file.write(chunk_bytes)
            else:
                with open(t_filepath, 'w') as file:
                    file.writelines(chunk)

            chunk_number += 1


    def split_n_chunks(self, n_chunks = 10, compress = False):

        chunk_size = (self.__estimate_csv_rows(self.filepath, self.header) // n_chunks)
        print("chunk_size:", chunk_size)

        chunk_number = 0              
        for chunk in self.__split_process(self.filepath, chunk_size):

            format = 'csv'

            if compress:
                format = 'csv.gz'

            t_filename = f'{self.prefix}_{chunk_number}.{format}'
            t_filepath = os.path.join(self.folder, t_filename)

            if compress:
                chunk_bytes = gzip.compress(''.join(chunk).encode()[6:])
                with open(t_filepath, 'wb') as chunk_file:
                    chunk_file.write(chunk_bytes)
            else:
                with open(t_filepath, 'w') as file:
                    file.writelines(chunk)

            chunk_number += 1


    def __split__columns_process(self, filepath, column_name, chunk_size = 10):
        
        partitions = {}
        row_number = 0        

        with open(filepath) as f:

            if self.header:
                header = next(f)
                _header =  header.strip().split(self.sep)

            index_column = _header.index(column_name)
            del _header[index_column]
            _header = self.sep.join(_header) + '\n'

            for line in f:
                row = line.strip().split(self.sep)
                value = row[index_column]
                del row[index_column]
                if value not in partitions:
                    if self.header:
                        partitions[value] = [_header]
                    else:
                        partitions[value] = []

                partitions[value].append(self.sep.join(row) + '\n')
                row_number += 1
                
                if row_number == chunk_size:
                    yield partitions
                    row_number = 0
                    partitions = {}                 

        if row_number > 0:
            yield partitions

    def split_by_column(self, column_value, batches = 10):

        chunk_size = (self.__estimate_csv_rows(self.filepath, self.header) // batches)   
        files = {}        
        format = 'csv'        

        for chunk in self.__split__columns_process(self.filepath, column_value, chunk_size):
            
            with concurrent.futures.ThreadPoolExecutor() as executor:

                tasks = []
                for value in chunk:                    

                    sufix = value.strip('"')                             
                    t_filepath = os.path.join(self.folder, f'{self.prefix}_{sufix}.{format}')
                    
                    if t_filepath not in files:
                        rows = chunk[value]
                        files[t_filepath] = open(t_filepath, 'a')
                    else:
                        rows = chunk[value][1:]
                                
                    def write_rows(file, name, rows):
                        try:
                            print("writing", len(rows),  "rows in", name)
                            file.writelines(rows)
                            file.flush()
                        except Exception as er:
                            print(er)
                    
                    tasks.append(executor.submit(write_rows, files[t_filepath], t_filepath, rows))
                
                concurrent.futures.wait(tasks)
        

        for file in files.values():
            file.close()
                                        



csvspl = CSVSplit('csvData.csv', 'data', 'chunks/', header = True, sep=',')
csvspl.split_by_column('country', 10)
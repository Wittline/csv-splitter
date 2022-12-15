import os
import gzip
import mmap

class CSVSplit:

    def __init__(self, filepath, prefix, folder, header = True):
        self.filepath = filepath
        self.header = header
        self.prefix = prefix
        self.folder = folder


    def __estimate_csv_rows(self, filename, header = True):

        count_rows = 0

        with open(filename, mode="r", encoding = "ISO-8859-1") as file_obj:

            with mmap.mmap(file_obj.fileno(), length=0, access=mmap.ACCESS_READ) as map_file:

                buffer = map_file.read(1<<13)
                file_size = os.path.getsize(filename)
                count_rows = file_size // (len(buffer) // buffer.count(b'\n')) - (1 if header else 0) 

        print("count_rows:", count_rows)
        return count_rows        

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


csvspl = CSVSplit('csvData.csv', 'data', 'chunks/', header = True)
csvspl.split_n_chunks(20, compress = False)
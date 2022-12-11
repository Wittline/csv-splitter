import os
import gzip

class CSVSplit:

    def __init__(self, filepath, prefix, folder, header = True):
        self.filepath = filepath
        self.header = header
        self.prefix = prefix
        self.folder = folder

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
    
    def split(self, chunk_size = 10, compress = False):

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

csvspl.split(100, compress = True)






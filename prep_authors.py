import pandas as pd

# Number of authors
N=1000
# Number of authorships
M=500000

# Read input files
authors = pd.read_csv (r'output_author.csv', sep=';', header=None, dtype='unicode')
authored_by = pd.read_csv (r'output_author_authored_by.csv', sep=';', header=None, dtype='unicode')

# Write
authors[:1000].to_csv('authors.csv',header=False, index=False)
authored_by[:200000].to_csv('authored_by.csv',header=False, index=False)
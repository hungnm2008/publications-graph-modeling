import pandas as pd
import numpy as np

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',12)

##### Select and create new proceedings file
# new_proceedings = proceedings.iloc[:,[0,14,27]]

# Read input files
proceedings = pd.read_csv ('output_proceedings.csv', sep=';', header=None, dtype='unicode')
inproceedings = pd.read_csv (r'output_inproceedings.csv', sep=';', header=None, dtype='unicode')

# Select relevant columns
new_inproceedings = inproceedings.iloc[:,[0,4,24,28]]
new_inproceedings.columns = ['articleID', 'conferenceID', 'articleTitle', 'year']

# Create edition = year + conferenceID
editions = new_inproceedings['year'] + " " + new_inproceedings['conferenceID'].astype(str)

# Create abstract randomly
abstracts = pd.util.testing.rands_array(100, len(new_inproceedings))
df_abstracts = pd.DataFrame(abstracts)

# Concat to form the final dataframe
conf_edition_year = pd.concat([new_inproceedings, df_abstracts, editions], axis=1)

#
topic_keyword = {'Computer Science':'bigdata','Computer Science':'hadoop','Physics':'Amplitude','Physics':'Quantum '}
conf_edition_year['topic'] = np.random.choice(list(topic_keyword), len(conf_edition_year))
conf_edition_year['keywords'] = conf_edition_year['topic'].map(topic_keyword)

# Write to file
conf_edition_year.to_csv('article_conf_title_year_abstract_edition_topic_keywords.csv',header=False, index=False)

print(conf_edition_year[:5])

# Create topics and keywords randomly




# conferences.to_csv('conferences.csv',header=False, index=False)


# print (new_proceedings[:5])

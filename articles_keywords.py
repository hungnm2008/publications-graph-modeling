import pandas as pd
import numpy as np

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',12)

# Number of samples for each categories (conferences/journals)
N=5000

# Read input files
# proceedings = pd.read_csv ('output_proceedings.csv', sep=';', header=None, dtype='unicode')
inproceedings = pd.read_csv (r'output_inproceedings.csv', sep=';', header=None, dtype='unicode')
journals = pd.read_csv (r'output_article.csv', sep=';', header=None, dtype='unicode')

# Select relevant columns
new_inproceedings = inproceedings.iloc[:,[0]]
new_inproceedings.columns = ['articleID']
new_journals = journals.iloc[:,[0]]
new_journals.columns = ['articleID']


# Randomize keywords and topics
keyword_topic = pd.DataFrame(columns=['keyword', 'topic'],
                             data=[['data management', 'Computer Science'],
                                    ['indexing', 'Computer Science'],
                                    ['data modeling','Computer Science'],
                                    ['big data','Computer Science'],
                                    ['data processing','Computer Science'],
                                    ['data storage','Computer Science'],
                                    ['data querying','Computer Science'],
                                    ['hadoop','Computer Science'],
                                    ['Amplitude','Physics'],
                                    ['Quantum','Physics']])

# Sampling articles id and keywords id with replacement
sample_articles_conf = new_inproceedings.iloc[:,[0]].sample(n=N,replace=True)
sample_articles_conf.reset_index(drop=True, inplace=True)

sample_articles_jour = new_journals.iloc[:,[0]].sample(n=N,replace=True)
sample_articles_jour.reset_index(drop=True, inplace=True)

sample_keywords_index = np.random.choice(keyword_topic.index.values,2*N,replace=True)
sample_keywords = pd.DataFrame(sample_keywords_index, index=None)

# Concat and write to file
sample_articles = pd.concat([sample_articles_conf, sample_articles_jour], axis=0, ignore_index=True)

article_keyword = pd.concat([sample_articles, sample_keywords], axis=1, ignore_index=True)
article_keyword.to_csv('article_keyword.csv',header=False, index=False)
keyword_topic.to_csv('keyword_topic.csv',header=False, index=True)

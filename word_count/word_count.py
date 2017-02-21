import re
import nltk
import pandas as pd
from pyspark import SparkContext
from nltk.corpus import stopwords

def preprocess_to_get_words(text_file_rdd):
	words=re.sub('[^a-z| |0-9]', '', text_file_rdd.strip().lower()).split(" ")
	eng_stopwords=stopwords.words('english')
	return [word for word in words if word not in eng_stopwords and len(word)>1]
	
if __name__=="__main__":

	sc = SparkContext("local","Application")
	
	text_file_rdd = sc.textFile("./../data/WordCountData.txt")
	allWords = text_file_rdd.flatMap(preprocess_to_get_words).map(lambda aWord: (aWord,1))
	unqWord_freq_tuples_list = allWords.reduceByKey(lambda v1,v2: v1+v2)
	
	print "\nTotal distinct words:"+str(unqWord_freq_tuples_list.count())+"\n"
	
	df = pd.DataFrame(sorted(unqWord_freq_tuples_list.collect()), columns=['Word', 'Count'])
	df.sort_values([ 'Count'], ascending=[False], inplace=True)
	df_top_n = df.head(20)
	%matplotlib inline
	df_top_n.plot(kind='bar', x=df_top_n['Word'])
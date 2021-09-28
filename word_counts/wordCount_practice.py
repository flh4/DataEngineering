import string
from collections import Counter

def read_data(file):
	with open (file, 'r') as f:
		data = f.read().split()
	return data 

#Remove all special characters
def clean(data_list):
	p= string.punctuation
	new_data = [''.join(j for j in word if j not in p) for word in data_list]
	return new_data

def de_capitalize(data_list):
	lower_case = [w.lower() for w in data_list]
	return lower_case

#Count using Dictionary
def count_words(data_list):
	d = {}
	for word in data_list:
		if word not in d:
			d[word] = 0
		d[word] += 1
	return translate(d)

#Count using collections.Counter
def count_words_2(data_list):
	count = Counter
	c = count(data_list)
	return translate(c) 


def translate(d):
	freq = []
	for k, v in d.items():
		freq.append((v,k))
	freq.sort(reverse = False)
	return freq

def out(data):
	with open('out.txt', 'w') as f:
		for i in data:
			f.write(' '.join (str(s) for s in i) + '\n')

def main():
	in_file = "Shakespeare.txt"
	my_data = read_data(in_file)
	print(my_data)
	cleaned_data = clean(my_data)
	de_capitalized_data = de_capitalize(cleaned_data)
	counted = count_words(de_capitalized_data)
	#counted = count_words_2(de_capitalized_data)
	print(counted)
	out(counted)

main()


for asdfasdf:



for 
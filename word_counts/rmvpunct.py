






new = []

words = f.read().split()

exclude = set(string.punctuation)

for s in words:
	s = ''.join(ch for ch in s if ch not in exclude)
	new.append(s)


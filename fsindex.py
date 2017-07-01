# FSIndex
# Indexes the filesystem for quick searching

import os, sys, hashlib, pickle, json, ctypes, re, base64, time
from collections import deque

index = {}

def startIndexing(root):
	global index
	print "Starting index from " + root
	
	queue = deque(os.listdir(root))
	n = 0
	last10 = time.time()
	itempersec = 0
	while len(queue) > 0:
		n += 1
		#now = queue.popleft()
		now = queue.pop()
		#print now
		
		#if n % 10 == 0:
		print "[" + str(n) + " scanned, " + str(len(queue)) + " left] " + root + now
		
		title("Indexing... {0} scanned, {1} left, at {2} files/sec".format(n, len(queue), int(itempersec)))
		
		if os.path.isfile(root + now):
			indexFile(root + now)
		else:
			try:
				dirs = map(lambda dir: now + "\\" + dir, os.listdir(root + now))
				queue.extend(dirs)
			except:
				pass
		
		if n % 10 == 0:
			timeto10 = time.time() - last10
			last10 = time.time()
			itempersec = 10 / timeto10
		
		if n % 1000 == 0:
			if n % 5000 == 0:
				# periodic backup
				idxf = open("index.{0}.pickle".format(time.time()), "wb")
				pickle.dump(index, idxf)
				idxf.close()
			# periodic save
			idxf = open("index.pickle", "wb")
			pickle.dump(index, idxf)
			idxf.close()
	
	print "Finished indexing"

def loadIndex():
	global index
	print "Loading index"
	a = open("index.pickle", "rb")
	index = pickle.load(a)
	a.close()
	print "{0} entries loaded".format(len(index))

def indexFile(path):
	global index
	try:
		f = open(path, "rb")
		
		#name = os.path.basename(path)
		size = os.path.getsize(path)
		hash = makeHashes(f)
		
		# TODO: Index media metadata
		index[path] = (size, hash)
		f.close()
	except:
		return

def makeHashes(f):
	global index
	hash_md5 = hashlib.md5()
	f.seek(0, 2) # move to end of file
	sz = f.tell() # get size
	f.seek(0) # move back to beginning
	for chunk in iter(lambda: f.read(67108864), b""): # read in chunks of 64MB
		ptr = f.tell()
		if ptr > 100000000: # >100mb
			title("Hashing file... {0}/{1} ({2}%)".format(ptr, sz, 100*ptr/sz))
		hash_md5.update(chunk)
	
	return hash_md5.digest()

def displayItem(k):
	global index
	print "    {0}\n    MD5: {1} | Size: {2} bytes\n".format(k, base64.b16encode(index[k][1]), index[k][0])

def displaySearch():
	global index
	loadIndex()
	
	while True:
		print "Search query"
		query = raw_input("> ")
		doSearch(query)

def doSearch(query):
	global index
	
	start = time.time()
	n = 0
	found = 0
	total = len(index)
	if not (len(query) == 32): # or len(query) == 64 or len(query) == 128):
		print "Searching by file name: {0}".format(query)
		if query.startswith("/") and query.endswith("/"):
			print "Searching with RegExp\n\n"
			rx = re.compile(query[1:-1])
			for key in index:
				n += 1
				title("Searching: {0}/{1} ({2}%), found {3} items".format(n, total, n*100/total, found))
				if rx.match(key):
					found += 1
					displayItem(key)
				
		else:
			print "Doing caseless search\n\n"
			query = query.lower()
			for key in index:
				n += 1
				title("Searching: {0}/{1} ({2}%), found {3} items".format(n, total, n*100/total, found))
				if query in key.lower():
					found += 1
					displayItem(key)
	else:
		#x = {32: 0, 64: 1, 128: 2}[len(query)]
		raw = base64.b16decode(query.strip())
		#print "Searching by {0}".format(("MD5", "SHA256", "SHA512")[x])
		print "Searching by MD5: {0}\n\n".format(query)
		for key in index:
			n += 1
			title("Searching: {0}/{1} ({2}%), found {3} items".format(n, total, n*100/total, found))
			if index[key][1] == raw:
				found += 1
				displayItem(key)
	
	end = time.time()
	print "\nFinished searching, found {0} items in {1} seconds\n".format(found, end - start)

def displayMenu():
	global index
	title("FSIndex")
	print "FSIndex"
	print ""
	print "[ ! ] Start indexing (replaces old index)"
	print "[ s ] Search"
	print "[ x ] Exit"
	print ""
	choice = raw_input("> ")
	
	if choice == "!":
		print "Root directory?"
		startIndexing(raw_input("> "))
	elif choice == "s":
		displaySearch()

def title(title):
	ctypes.windll.kernel32.SetConsoleTitleA(title)

if __name__ == "__main__":
	if len(sys.argv) == 1:
		displayMenu()
	else:
		if sys.argv[1] == "search":
			loadIndex()
			doSearch(sys.argv[2])
	#startIndexing("D:\\")

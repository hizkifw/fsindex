"""
FSIndex
Indexes the filesystem for quick searching
(c) 2017 Hizkia Felix
"""

import os, sys, hashlib, pickle, json, ctypes, re, base64, time
from collections import deque

index = {}

def startIndexing(root):
	"""
	Indexes all files within `root`
	"""
	global index
	print "Starting index from " + root
	
	stack = deque(os.listdir(root))
	n = 0
	last10 = time.time()
	itempersec = 0
	while len(stack) > 0:
		# Get next target from stack
		now = stack.pop()
		n += 1
		
		# Show some status messages
		print "[{0} scanned, {1} left] {2}".format(n, len(stack), root + now)
		title("Indexing... {0} scanned, {1} left, at {2} files/sec".format(n, len(stack), int(itempersec)))
		
		if os.path.isfile(root + now):
			# If item is a file, add to index
			indexFile(root + now)
		else:
			try:
				# If item is a directory, try to list contents and add to stack
				dirs = map(lambda dir: now + "\\" + dir, os.listdir(root + now))
				stack.extend(dirs)
			except:
				pass
		
		if n % 10 == 0:
			# Calculate scan speed every 10 items
			timeto10 = time.time() - last10
			last10 = time.time()
			itempersec = 10 / timeto10
			
			# Saves the index every 1000 items
			if n % 1000 == 0:
				idxf = open("index.pickle", "wb")
				pickle.dump(index, idxf)
				idxf.close()
				
				# Do periodic backups every 5000 items
				# Just in case the program crashes when saving
				if n % 5000 == 0:
					idxf = open("index.{0}.pickle".format(time.time()), "wb")
					pickle.dump(index, idxf)
					idxf.close()
	
	print "Finished indexing"

def loadIndex():
	"""
	Loads entire index file into memory
	"""
	global index
	print "Loading index"
	a = open("index.pickle", "rb")
	index = pickle.load(a)
	a.close()
	print "{0} entries loaded".format(len(index))

def indexFile(path):
	"""
	Checks if the file is readable and adds it to the index
	"""
	global index
	try:
		f = open(path, "rb")
		
		size = os.path.getsize(path)
		hash = makeHashes(f)
		
		# TODO: Index media metadata
		index[path] = (size, hash)
		f.close()
	except:
		return

def makeHashes(f):
	"""
	Generates file hashes
	"""
	
	# Get file size from file object
	f.seek(0, 2) # move to end of file
	sz = f.tell() # get size
	f.seek(0) # move back to beginning
	
	# Calculates the MD5 checksum
	hash_md5 = hashlib.md5()
	for chunk in iter(lambda: f.read(67108864), b""): # read in chunks of 64MB
		ptr = f.tell()
		if sz > 100000000: # Show progress if file is >100Mb
			title("Hashing file... {0}/{1} ({2}%)".format(ptr, sz, 100*ptr/sz))
		hash_md5.update(chunk)
	
	return hash_md5.digest()

def displayItem(k):
	"""
	Outputs search results
	"""
	global index
	print "    {0}\n    MD5: {1} | Size: {2} bytes\n".format(k, base64.b16encode(index[k][1]), index[k][0])

def displaySearch():
	"""
	Search UI wrapper
	"""
	loadIndex()
	
	while True:
		print "Search query"
		query = raw_input("> ")
		doSearch(query)

def doSearch(query):
	"""
	Searches the index
	"""
	global index
	
	tStart = time.time()
	n = 0
	found = 0
	total = len(index)
	if not len(query) == 32:
		print "Searching by file path: {0}".format(query)
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
		bin = base64.b16decode(query.strip())
		print "Searching by MD5: {0}\n\n".format(query)
		for key in index:
			n += 1
			title("Searching: {0}/{1} ({2}%), found {3} items".format(n, total, n*100/total, found))
			if index[key][1] == bin:
				found += 1
				displayItem(key)
	
	tEnd = time.time()
	print "\nFinished searching, found {0} items in {1} seconds\n".format(found, tEnd - tStart)

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
	"""
	Sets console window title
	"""
	ctypes.windll.kernel32.SetConsoleTitleA(title)

if __name__ == "__main__":
	if len(sys.argv) == 1:
		# Display UI if no command line arguments are provided
		displayMenu()
	else:
		if sys.argv[1] == "-s" or sys.argv[1] == "--search":
			loadIndex()
			doSearch(sys.argv[2])
		else:
			print "Usage: python.exe fsindex.py [-s <query>]"
			print ""
			print "Options:"
			print "    -h, --help         Displays this help page"
			print "    -s, --search query Searches the index for a file path matching query. Regex"
			print "                       can be used by adding leading and trailing slashes"
			print ""

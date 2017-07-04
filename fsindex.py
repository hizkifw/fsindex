"""
FSIndex
Indexes the filesystem for quick searching
Currently only works on Windows and Python 2.7
(c) 2017 Hizkia Felix
"""

import os, sys, hashlib, pickle, json, ctypes, re, base64, time, threading, csv
from collections import deque

index = {}
mem = ""
nThreads = 16
isSaving = False

def startIndexing(root):
	"""
	Indexes all files within `root`
	"""
	global index, mem, nThreads, isSaving
	
	# Normalize file path
	root = re.sub(r"\\+", r"\\", root + "\\")
	print "Starting index from " + root
	print "Press Ctrl-C anytime to save and quit"
	
	loadIndex()
	
	stack = deque(os.listdir(root))
	n = 0
	last10 = time.time()
	itempersec = 0
	tIdxStart = time.time()
	saveThread = threading.Thread(target=dumpIndexToFile)
	indexThreads = [None]*nThreads
	exploreThread = threading.Thread(target=doExplore)
	lastSave = time.time()
	try:
		while len(stack) > 0:
			# Show some status messages
			title("Indexing... {0} scanned, {1} left, at {2} files/sec. Mem: {3}, Elapsed: {4} {5}".format(
				n, len(stack), int(itempersec), mem, sec2time(time.time() - tIdxStart), "[Saving]" if isSaving else ""
			))

			for i in range(nThreads):
				# Look for completed threads
				if indexThreads[i] is None or not indexThreads[i].isAlive() and len(stack) > 0:
					# Get next target from stack
					now = stack.pop()
					n += 1
					
					# Check target type
					if os.path.isfile(root + now):
						# Start thread if file
						indexThreads[i] = threading.Thread(target=indexFile, args=(root + now,))
						indexThreads[i].start()
						print "[{3:02d}][{0} scanned, {1} left] {2}".format(n, len(stack), root + now, i)
					else:
						# Add contents to stack if directory
						print "Exploring directory {0}".format(root + now)
						doExplore(root, now, stack)
			
			if n % 10 == 0:
				# Calculate scan speed every 10 items
				timeto10 = time.time() - last10
				last10 = time.time()
				itempersec = 10 / timeto10 if timeto10 > 0 else 0
				
				if n % 100 == 0:
					# Get memory usage every 100 items
					infoThread = threading.Thread(target=getMem)
					infoThread.start()
				
					if time.time() - lastSave > 600: # and not saveThread.isAlive():
						# Saves the index every 10 minutes
						# Background saving is too buggy
						print "Waiting for all threads to finish..."
						for i in range(nThreads):
							while indexThreads[i].isAlive():
								time.sleep(1)
						print "Saving..."
						lastSave = time.time()
						dumpIndexToFile()
						# saveThread = threading.Thread(target=dumpIndexToFile)
						# saveThread.start()
	except KeyboardInterrupt:
		# Ctrl-C
		print "Stopped by KeyboardInterrupt"
	
	tIdxEnd = time.time()
	tIdx = tIdxEnd - tIdxStart
	print "Finished indexing in {0} ({1:.2f} files/sec avg.)".format(sec2time(tIdx), n / tIdx)
	
	print "Waiting for all threads to finish..."
	for i in range(nThreads):
		while indexThreads[i].isAlive():
			time.sleep(1)
	
	if saveThread.isAlive():
		print "Waiting for previous save to complete"
		while saveThread.isAlive():
			time.sleep(1)
		
	# Blocking save at the end of indexing
	print "Saving..."
	dumpIndexToFile()
	print "Index file saved!"

def doExplore(root, now, stack):
	# If item is a directory, try to list contents and add to stack
	try:
		dirs = map(lambda dir: now + "\\" + dir, os.listdir(root + now))
		stack.extend(dirs)
	except:
		pass

def dumpIndexToFile():
	"""
	Safely dumps the index to a file
	"""
	global index, isSaving
	
	isSaving = True
	try:
		# Save to temp file
		idxf = open("index.pickle~", "wb")
		pickle.dump(index, idxf)
		idxf.close()
		
		# Replace original file with the temp file
		abspath = os.path.abspath("index.pickle~")
		try:
			os.remove(abspath[:-1])
		except:
			pass
		os.rename(abspath, abspath[:-1])
	except Exception, e:
		print "Unable to save file!"
		print str(e)
		errSave = e
	isSaving = False

def dumpIndexToCsv(out):
	"""
	Exports the index as a CSV file
	"""
	global index
	
	print "Exporting as CSV"
	with open(out, "wb") as f:
		c = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
		c.writerow(["path", "size", "hash", "modified"])
		n = 0
		t = len(index)
		for path in index:
			n += 1
			now = index[path]
			c.writerow([path, now[0], base64.b16encode(now[1]), now[2] if 2 in now else -1])
			
			if n % 1000 == 0:
				title("Exporting... {0}/{1} ({2}%)".format(n, t, 100*n/t))
	print "Done!"

def loadIndex():
	"""
	Loads entire index file into memory
	"""
	global index
	print "Loading index"
	try:
		a = open("index.pickle", "rb")
		index = pickle.load(a)
		a.close()
		print "{0} entries loaded".format(len(index))
	except:
		index = {}
		print "No index file found, creating empty"

def indexFile(path):
	"""
	Checks if the file is readable and adds it to the index
	"""
	global index
	try:
		f = open(path, "rb")
		
		size = os.path.getsize(path)
		modified = os.path.getmtime(path)
		
		if path in index and index[path][0] == size and len(index[path]) > 2 and index[path][2] == modified:
			# File is the same, don't hash
			#print "Skipping"
			f.close()
			return
		
		hash = makeHashes(f)
		
		# TODO: Index media metadata
		index[path] = (size, hash, modified)
		f.close()
	except:
		return

def makeHashes(f):
	"""
	Generates file hashes
	"""
	
	# Get file size from file object
	f.seek(0, 2)  # move to end of file
	sz = f.tell() # get size
	f.seek(0)     # move back to beginning
	
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

def doFindDuplicates():
	"""
	Finds duplicates by comparing hashes
	"""
	global index
	loadIndex()
	hashes = {}
	dupes = []
	n = 0
	t = len(index)
	
	print "Searching for hash duplicates..."
	print "Press Ctrl-C anytime to stop"
	try:
		for path in index:
			n += 1
			if index[path][1] in hashes:
				hashes[index[path][1]].append(path)
				if not index[path][1] in dupes:
					dupes.append(index[path][1])
			else:
				hashes[index[path][1]] = [path]
			
			if n % 1000 == 0:
				title("Finding duplicates... {0}/{1} ({2}%), found {3}".format(n, t, 100*n/t, len(dupes)))
	except KeyboardInterrupt:
		print "Stopped by KeyboardInterrupt"
	
	# Free up some RAM
	del index
	
	if len(dupes) > 0:
		for hash in dupes:
			print base64.b16encode(hash)
			for path in hashes[hash]:
				print "    " + path
			print ""
	else:
		print "No duplicates found"

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
				if n % 500 == 0:
					title("Searching: {0}/{1} ({2}%), found {3} items".format(n, total, n*100/total, found))
				if rx.match(key):
					found += 1
					displayItem(key)
				
		else:
			print "Doing caseless search\n\n"
			query = query.lower()
			for key in index:
				n += 1
				if n % 500 == 0:
					title("Searching: {0}/{1} ({2}%), found {3} items".format(n, total, n*100/total, found))
				if query in key.lower():
					found += 1
					displayItem(key)
	else:
		bin = base64.b16decode(query.upper().strip())
		print "Searching by MD5: {0}\n\n".format(query)
		for key in index:
			n += 1
			if n % 500 == 0:
				title("Searching: {0}/{1} ({2}%), found {3} items".format(n, total, n*100/total, found))
			if index[key][1] == bin:
				found += 1
				displayItem(key)
	
	tEnd = time.time()
	print "\nFinished searching, found {0} items in {1}\n".format(found, sec2time(tEnd - tStart))

def displayMenu():
	global index
	title("FSIndex")
	print "FSIndex"
	print ""
	print "[ i ] Start indexing"
	print "[ s ] Search"
	print "[ d ] Find duplicate files"
	print "[ e ] Export data as CSV"
	print "[ x ] Exit"
	print ""
	choice = raw_input("> ")
	
	if choice == "i":
		print "Root directory?"
		startIndexing(raw_input("> "))
	elif choice == "s":
		displaySearch()
	elif choice == "d":
		doFindDuplicates()
	elif choice == "e":
		loadIndex()
		print "Output file?"
		dumpIndexToCsv(raw_input("> "))

def title(title):
	"""
	Sets console window title
	"""
	ctypes.windll.kernel32.SetConsoleTitleA(title)

def sec2time(secs):
	"""
	Converts seconds to hours, minutes, and seconds
	"""
	s = secs % 60
	m = int(secs // 60 % 60)
	h = int(secs // 3600)
	return "{0:02d}h {1:02d}m {2:.2f}s".format(h, m, s)

def getMem():
	"""
	Gets memory usage using tasklist
	"""
	global mem
	pid = os.getpid()
	exe = 'tasklist /fi "pid eq {0}" /fo csv /nh'.format(pid)
	csv = os.popen(exe).read()
	mem = re.match(r"(\"(.*)\"\,?){5}", csv).group(2)
	return mem

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

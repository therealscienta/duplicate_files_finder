#!/usr/bin/env python

import re
import os
import sys
import csv
import time
import hashlib
import argparse
import mimetypes
from random import randrange

def ChunkReader(fobj, chunk_size=1024):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk

def GetHash(filename, first_chunk_only=False, hash=hashlib.sha1):
    """Generate hash for input file"""
    hashobj = hash()
    file_object = open(filename, 'rb')

    if first_chunk_only:
        hashobj.update(file_object.read(1024))
    else:
        for chunk in ChunkReader(file_object):
            hashobj.update(chunk)
    hashed = hashobj.digest()

    file_object.close()
    return hashed

def CheckFileType(path):
    """Check mimetype and/or file extension to detect valid video file"""
    fileMimeType, encoding = mimetypes.guess_type(path)
    fileExtension = path.rsplit('.', 1)

    if fileMimeType is None:
        if len(fileExtension) >= 2 and fileExtension[1].lower() not in ['jpg', 'jpeg', 'png', 'gif', 'pdf', 'tif', 'svg', 'bmp', 'heif']:
            return False
    else:
        fileMimeType = fileMimeType.split('/', 1)
        if fileMimeType[0] != 'image':
            return False
    return True

def CheckForDuplicates(paths, hash=hashlib.sha1):
    """Main function that search for duplicates and return dictionary of results"""
    hashes_by_size = {}
    hashes_on_1k = {}

    # Dictionary that use hash as key and all files with
    # the same hash as values
    hashes_full = {}

    size_duplicates = 0
    sorted_dict = {}
    path_list = [paths]

    for path in path_list:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)

                try:
                    # if the target is a symlink (soft one), this will 
                    # dereference it - change the value to the actual target file
                    full_path = os.path.realpath(full_path)
                    file_size = os.path.getsize(full_path)
                except (OSError,):
                    # not accessible (permissions, etc) - pass on
                    continue

                # Only use file of specified filetype
                if not CheckFileType(full_path):
                    continue

                duplicate = hashes_by_size.get(file_size)

                if duplicate:
                    hashes_by_size[file_size].append(full_path)
                else:
                    hashes_by_size[file_size] = []  # create the list for this file size
                    hashes_by_size[file_size].append(full_path)

    # For all files with the same file size, get their hash on the 1st 1024 bytes
    for __, files in hashes_by_size.items():
        if len(files) < 2:
            continue    # this file size is unique, no need to spend cpu cycles on it

        for filename in files:
            try:
                small_hash = GetHash(filename, first_chunk_only=True)
            except (OSError,):
                # the file access might've changed till the exec point got here 
                continue

            duplicate = hashes_on_1k.get(small_hash)
            if duplicate:
                hashes_on_1k[small_hash].append(filename)
            else:
                hashes_on_1k[small_hash] = []          # create the list for this 1k hash
                hashes_on_1k[small_hash].append(filename)

    # For all files with the hash on the 1st 1024 bytes, get their hash on the full file - collisions will be duplicates
    for __, files in hashes_on_1k.items():
        if len(files) < 2:
            continue    # this hash of fist 1k file bytes is unique, no need to spend cpu cycles on it

        for filename in files:
            try: 
                full_hash = GetHash(filename, first_chunk_only=False)
            except (OSError,):
                # the file access might've changed till the exec point got here 
                continue

            # If duplicate is found, print out and add files to hashed list
            duplicate = hashes_full.get(full_hash)
            if duplicate:
                hashes_full[full_hash].append(filename)
            else:
                hashes_full[full_hash] = [filename]

    # Sort through dictionary to determine probable original files
    for items in hashes_full.keys():

        # List of files that are probably not original, will be appended
        # to sorted_dict with original filename as key
        dup_list = []

        while len(hashes_full[items]) > 0:

            # When the list only has one item left, that is used
            # as the original file
            if len(hashes_full[items]) == 1:
                sorted_dict[hashes_full[items][0]] = dup_list
                hashes_full[items].pop()

            else:
                # X and Y are the files that will be compared,
                # the most probable candidate remains in the list
                x = hashes_full[items][0]
                y = hashes_full[items][1]
                size_duplicates += os.path.getsize(x)
                fileage = round(time.time() - os.path.getmtime(x))
                dupage = round(time.time() - os.path.getmtime(y))

                # If "copy" in filename, dump item to duplicate list
                if re.search("(C|c)(opy|OPY)",x) or re.search("(C|c)(opy|OPY)",y):
                    if re.search("(C|c)(opy|OPY)",x) and not re.search("(C|c)(opy|OPY)",y):
                        dup_list.append(x)
                        hashes_full[items].remove(x)
                        continue
                    elif re.search("(C|c)(opy|OPY)",y) and not re.search("(C|c)(opy|OPY)",x):
                        dup_list.append(y)
                        hashes_full[items].remove(y)
                        continue

                # If (n) in filename, dump item to duplicate list
                if re.search("\(\d+\)",x) or re.search("\(\d+\)",y):
                    if re.search("\(\d+\)",x) and not re.search("\(\d+\)",y):
                        dup_list.append(x)
                        hashes_full[items].remove(x)
                        continue
                    elif re.search("\(\d+\)",y) and not re.search("\(\d+\)",x):
                        dup_list.append(y)
                        hashes_full[items].remove(y)
                        continue

                # If IMG in filename, dump item to duplicate list
                if re.search("(I|i)(mg|MG)",y) or re.search("(I|i)(mg|MG)",x):
                    if re.search("(I|i)(mg|MG)",x) and not re.search("(I|i)(mg|MG)",y):
                        dup_list.append(y)
                        hashes_full[items].remove(y)
                        continue
                    elif re.search("(I|i)(mg|MG)",y) and not re.search("(I|i)(mg|MG)",x):
                        dup_list.append(x)
                        hashes_full[items].remove(x)
                        continue

                # If e.g. 20190515 in filename, dump item to duplicate list
                if re.search("20\d{2}\d{4}",y) and not re.search("20\d{2}\d{4}",x):
                    if re.search("20\d{2}\d{4}",x) and not re.search("20\d{2}\d{4}",y):
                        dup_list.append(x)
                        hashes_full[items].remove(x)
                        continue
                    elif re.search("20\d{2}\d{4}",y) and not re.search("20\d{2}\d{4}",x):
                        dup_list.append(y)
                        hashes_full[items].remove(y)
                        continue

                # If no other condition apply, use oldest file as original
                if fileage < dupage:
                    dup_list.append(x)
                    hashes_full[items].remove(x)
                else:
                    dup_list.append(y)
                    hashes_full[items].remove(y)

    if len(sorted_dict) > 0:
        print(f"\nFound duplicates for a total of {round(size_duplicates/1024/1024,1)}MB. Results was printed to CSV-file.")
    return sorted_dict

### Main script execution ###

script_path = sys.argv[0][:-2]
csv_path = script_path + "csv"

# If there exists a csv-file, ask if user want to delete duplicates from file
if os.path.isfile(csv_path) is True and os.path.getsize(csv_path) != 0:
    
    print("CSV file found.")
    while True:
        print("""
        What do you want to do with the file?
        [1] Print content
        [2] Delete duplicate files
        [3] Exit
                """)
        # print()
        answer = str(input("Answer(1-3): ")).strip()
        if answer == "3":
            sys.exit()
        if answer  == "1" or answer == "2":
            break
        else:
            print("Please give a valid answer.")

    # Make sure the user really wanted to delete
    if answer == "2":
        print("Are you sure you want to delete failes? Type \"yes\" to continue...\n")
        second_answer = input("Answer: ")
        if second_answer.lower() != "yes":
            print("Exiting")
            sys.exit()

    # Print text to screen with delay    
    text = "Analysing content.....\n"
    for c in text:
        sys.stdout.write(c)
        sys.stdout.flush()
        if c == ".":
            seconds = "0." + str(randrange(2, 5, 1))
            seconds = float(seconds)
            time.sleep(seconds)
    
    item_count = 0
    lines_count = 0
    failed_deletes = []

    with open(csv_path,'r') as csvread:

        csvreader = csv.reader(csvread, delimiter=";")
        for line in csvreader:
            lines_count += 1
            if not lines_count == 1:
                path = line[1]
                item_count += 1

                # Print items in csv-file
                if answer == "1":
                    print(f"{line[0]};{line[1]}")

                # Delete duplicate failes, i.e. files in second column
                # in csv-file, if the exist
                elif answer == "2":
                    print(f"Trying to delete {path}")
                    try:
                        if os.path.isfile(path):
                            os.remove(path)
                        else:
                            raise OSError("File does not exist")
                    except:
                        failed_deletes.append(path)
                        print(f"ERROR: Could not locate file: {path}")
    if answer == "2":
        # Delete csv-file when done
        os.remove(csv_path)
        print(f"Attempted to delete {item_count} duplicates from list. {len(failed_deletes)} files failed to delete.")

    # If any files failed to delete, recreate csv-file
    if failed_deletes:
        with open(csv_path,'w') as csvfile:
            csvwrite = csv.writer(csvfile, delimiter=";")
            for value in failed_deletes:
                row_to_write = ["failed", value]
                csvwrite.writerow(row_to_write)
        print("Files that could not be deleted are kept in csv-file")

    # Exit when done with csv file
    sys.exit()
else:
    if os.path.isfile(csv_path) and os.path.getsize(csv_path) == 0: 
        os.remove(csv_path)
    print("No CSV file found.")

# Check if user provided a path to search
if not len(sys.argv) > 1:
    print("Please pass the path to search for duplicates as parameter to the script")
    sys.exit()

# Check for passed parameters
if os.path.exists(sys.argv[1]):

    print("Searching for duplicates...")
    dict_of_duplicates = CheckForDuplicates(sys.argv[1])

    # Check if any duplicates were found
    if len(dict_of_duplicates) > 0:
        topField = ["filename", "duplicate"]
        filePath = sys.argv[0][:-2] + "csv"
        
        # Create csv-file with files and duplicates
        with open(filePath,'w') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=";")
            csvwriter.writerow(topField)

            for key in dict_of_duplicates.keys():
                for value in dict_of_duplicates[key]:
                    row_to_write = [key, value]
                    csvwriter.writerow(row_to_write)
    else:
        print("No duplicates were found.")
        sys.exit()
        
else:
    print("The provided path is invalid.")
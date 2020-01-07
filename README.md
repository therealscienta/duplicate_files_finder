# duplicate_files_finder

Original functions found at https://stackoverflow.com/questions/748675/finding-duplicate-files-and-removing-them by user Todor Minakov.

This script adds additional features and an algorithm to sort out which file is most likely to be source file.
The script currently only look for image filetypes and then sort through candidates to determine
most probable original file. 

Usage:

Run script and pass filepath to be scanned for duplicates

E.g. duplicate_files_finder.py /my/path/to/search

If duplicates are found, a csv-file will be generated with the same name as the script file.
Run the script again (searchpath not needed) to handle duplicates. Options allow user
to print content of file or delete the duplicate files.



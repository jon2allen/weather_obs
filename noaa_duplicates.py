import os
import sys
import hashlib
import pprint
from obs_utils import get_noaa_text_files
####################################################
#  program:  find duplicate noaa marine forcecasts
#
#  Uses MD5 of file to find duplicate
#
####################################################

# SET DIR to directory to find the data
DIR = "./data/"
# DIR = "." ( same dir as program 
def summary_noaa_files(noaa_files, dupe_candidate):
    print("**** SUMMARY *****")
    print("Total number of files: ", str(len(noaa_files)))
    print("Num of duplicates:", str(len(dupe_candidate)))


if __name__ == "__main__":
    noaa_files = get_noaa_text_files(DIR, "ANZ535")
    dupe_candidate = []
    last_md5 = ""
    for file in noaa_files:
        if "latest" in file:
            continue
        with open(DIR +  file, "rb") as fl:
            blob1 = fl.read()
            curr_md5 = str(hashlib.md5(blob1).hexdigest())
            print("file: ", file, " md5:", curr_md5)
            if (curr_md5 == last_md5):
                dupe_candidate.append((file, curr_md5))
            else:
                last_md5 = curr_md5
    #
    print("Duplicate candidate list")
    if (len(dupe_candidate) == 0):
        print("No duplicates found")
        summary_noaa_files(noaa_files, dupe_candidate)
        sys.exit(0)
    pprint.pprint(dupe_candidate)
    print(type(dupe_candidate[0]))
    summary_noaa_files(noaa_files, dupe_candidate)

    ans = input("Proceed with delete: [y/n]: ")

    print(ans)

    if (ans == 'y'):
        for file in dupe_candidate:
            print("Deleting:  ", file[0])
            os.remove(file[0])

    print("completed... ")

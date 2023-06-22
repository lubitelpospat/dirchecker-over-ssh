#!/usr/bin/env python3

import getpass
import argparse
import fabric
import tqdm
import sys
import pathlib
import subprocess
import hashlib



parser = argparse.ArgumentParser(
    description="Script that checks if directories presented in the BASE_DIR are present on the REMOTE_HOST in REMOTE_DIR"
    )


# Required arguments
parser.add_argument("BASE_DIR")
parser.add_argument("REMOTE_HOST")
parser.add_argument("REMOTE_DIR")


# Optional arguments
parser.add_argument("-p", "--pattern", default="*", help="run(sample) directory pattern")
parser.add_argument("-n", "--n-connections", default=10, type=int, help="Number of concurrent ssh connections")

args = parser.parse_args()


password = getpass.getpass("Remote password: ")

matching_dirs = []
nonmatching_dirs = []
with  fabric.Connection(host=args.REMOTE_HOST, connect_kwargs={"password": password}) as conn:
    sample_dirs = conn.run(f"ls {args.REMOTE_DIR}", hide='both')
    if not sample_dirs.ok:
        print(f"Failed to list directories in {args.REMOTE_DIR}")
        sys.exit(1)

    dirnames = sample_dirs.stdout.strip().split() # directories on remote host in REMOTE_DIR
    localdirs = list(map(lambda x: x.name,pathlib.Path(args.BASE_DIR).glob(args.pattern))) #directories on local host in REMOTE_HOST
    localdirs_present_on_remote_server = set(localdirs).intersection(set(dirnames))

   

    if len(localdirs_present_on_remote_server) == 0:
        print(f"No directories in {args.BASE_DIR} found in {args.REMOTE_HOST}:{args.REMOTE_DIR}")
    else:
        if len(localdirs_present_on_remote_server) == 1:
            print(f"1 local directory found on remote, need to compare hashes:")
            print(list(localdirs_present_on_remote_server)[0])
        else:    
            print(f"Need to compare hashes of {len(localdirs_present_on_remote_server)} directories:")
            print(",".join(localdirs_present_on_remote_server))
        for d in tqdm.tqdm(localdirs_present_on_remote_server): 
            hashes_command = conn.run(f"find {args.REMOTE_DIR}/{d}  -type f \( -name '*.fastq.gz' -o -name '*.fastq' -o -name '*.fast5' \) | head -n 10 | parallel -j10 sha256sum | awk '{{print $1}}'", hide='both')
            hashes = "#".join(sorted(hashes_command.stdout.strip().split("\n")))
            local_hashes_command = subprocess.run(
                f"find {args.BASE_DIR}/  -type f \( -name '*.fastq.gz' -o -name '*.fastq' -o -name '*.fast5' \)| head -n 10 | parallel -j10 sha256sum | awk '{{print $1}}'", 
                                                  capture_output=True, 
                                                  shell=True)
            local_hashes = "#".join(sorted(str(local_hashes_command.stdout, encoding="utf-8").split("\n")))

            remote_hash = hashlib.sha256()
            remote_hash.update(str.encode(hashes))
            local_hash = hashlib.sha256()
            local_hash.update(str.encode(local_hashes))
            if local_hash.hexdigest() == remote_hash.hexdigest():
                matching_dirs.append(d)
            else:
                nonmatching_dirs.append(d)

            

    
if len(nonmatching_dirs) == 0:
    print("All local directories are found remotely")

else:
    print(f"{len(nonmatching_dirs)} are not complete on remote destination:")
    print("\n".join(nonmatching_dirs))
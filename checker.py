#!/usr/bin/env python3

import getpass
import argparse
import fabric
import tqdm
import sys
import pathlib
import subprocess
import hashlib
import colorama
import joblib
from dataclasses import dataclass
from enum import Enum
from colorama import Fore
from typing import List, Tuple
from collections import defaultdict

colorama.init(autoreset=True)


class TaskType(Enum):
    LOCAL  = 1
    REMOTE = 2

@dataclass
class Task:
    type:       TaskType # whether the checks should be run on local or remote
    dirname:    str      # name of run directory that will be checked
    base_dir:   str      # base directory in which the run directory (`dirname`) can be found


@dataclass 
class TaskResult:
    type: TaskType
    dirname: str
    hash_val: str

def process_local_task(base_dir:str, dirname:str)->str:
    #computes hash locally

    # compute hash for all fastq, fastq,gz, fast5 and csv files, sort them (for reproducibility)
    local_hashes_command = subprocess.run(
                f"find {base_dir}/{dirname}  -type f \( -name '*.fastq.gz' -o -name '*.fastq' -o -name '*.fast5' -o -name '*.csv' \)|  parallel -j10 sha256sum | awk '{{print $1}}' | sort", 
                                                  capture_output=True, 
                                                  shell=True)
    
    # join hashes into one big string with # character
    local_hashes = "#".join(sorted(str(local_hashes_command.stdout.strip(), encoding="utf-8").split("\n"))) 
    local_hash = hashlib.sha256()
    local_hash.update(str.encode(local_hashes))
    return local_hash.hexdigest()


    
def process_remote_task(base_dir:str, dirname:str)->str:
    with  fabric.Connection(host=args.REMOTE_HOST, connect_kwargs={"password": PASSWORD}) as conn:
        hashes_command = conn.run(
            f"find {base_dir}/{dirname}  -type f \( -name '*.fastq.gz' -o -name '*.fastq' -o -name '*.fast5'  -name '*.csv' \) |  parallel -j10 sha256sum | awk '{{print $1}}' | sort", hide='both'
            )
        
        hashes = "#".join(sorted(hashes_command.stdout.strip().split("\n")))
    remote_hash = hashlib.sha256()
    remote_hash.update(str.encode(hashes))
    return remote_hash.hexdigest()


def process_task(task:Task)->TaskResult:
    if task.type == TaskType.LOCAL:
        h = process_local_task(task.base_dir, task.dirname)

    elif task.type == TaskType.REMOTE:
        h = process_remote_task(task.base_dir, task.dirname)

    else:
        raise ValueError(f"Unexpected TaskType: {task.type}")


    result = TaskResult(task.type, task.dirname, h)
    return result
        
        
        
def match_hashes(results: List[TaskResult])->Tuple[List[str], List[str]]:
    locals = list(filter(lambda x: x.type ==TaskType.LOCAL, results))
    remotes = list(filter(lambda x: x.type ==TaskType.LOCAL, results))

    matching = list()
    nonmatching = list()
    for local  in locals:
        ldir = local.dirname
        matching_remote_candidates = list(filter(lambda x: x.dirname == ldir, remotes))
        if len(matching_remote_candidates) == 0:
            raise ValueError(f"No remote result found for {ldir}")
        
        if len(matching_remote_candidates) > 1:
            raise ValueError(f"Too many remote candidates found for {ldir}: expected 1, got {len(matching_remote_candidates)}: {','.join([cand.dirname for cand in matching_remote_candidates])}")
        remote = matching_remote_candidates[0]

        if remote.hash_val == local.hash_val:
            matching.append(ldir)

        else:
            nonmatching.append(ldir)


    return (matching, nonmatching)


    



parser = argparse.ArgumentParser(
    description="Script that checks if directories presented in the BASE_DIR are present on the REMOTE_HOST in REMOTE_DIR"
    )


# Required arguments
parser.add_argument("BASE_DIR", help="Local directory")
parser.add_argument("REMOTE_HOST", help="ssh-style remote user and host name, e.g. john@google.com")
parser.add_argument("REMOTE_DIR", help="directory on the remote server")


# Optional arguments
parser.add_argument("-p", "--pattern", default="*", help="run(sample) directory pattern")
parser.add_argument("-n", "--n-jobs", help="Number of concurrent sha256 jobs that will be executed ", default=10, dest='n')
parser.add_argument("-i", "--ignore", type=str, dest="ignore", help="directories to ignore", default="")

args = parser.parse_args()


PASSWORD = getpass.getpass("Remote password: ")


with  fabric.Connection(host=args.REMOTE_HOST, connect_kwargs={"password": PASSWORD}) as conn:
    sample_dirs = conn.run(f"ls {args.REMOTE_DIR}", hide='both')
    if not sample_dirs.ok:
        print(f"Failed to list directories in {args.REMOTE_DIR}")
        sys.exit(1)
    if len(args.ignore) > 0:
        dirs_to_ignore = set(args.ignore.strip().split(","))
    dirnames = sample_dirs.stdout.strip().split() # directories on remote host in REMOTE_DIR
    localdirs = list(filter(lambda x: not (x in dirs_to_ignore), map(lambda x: x.name,pathlib.Path(args.BASE_DIR).glob(args.pattern)))) #directories on local host in REMOTE_HOST
    localdirs_present_on_remote_server = set(localdirs).intersection(set(dirnames))
    localdirs_not_found_on_remote_server = set(localdirs).difference(set(dirnames))
    print(Fore.RED + f"{len(localdirs_not_found_on_remote_server)} folder(s) not found on remote server: {','.join(localdirs_not_found_on_remote_server)}")
   

    if len(localdirs_present_on_remote_server) == 0:
        print(Fore.RED + f"No directories in {args.BASE_DIR} found in {args.REMOTE_HOST}:{args.REMOTE_DIR}")
    else:
        if len(localdirs_present_on_remote_server) == 1:
            print(f"1 local directory found on remote, need to compare hashes:")
            print(list(localdirs_present_on_remote_server)[0])
        else:    
            print(f"Need to compare hashes of {len(localdirs_present_on_remote_server)} directories:")
            print(",".join(localdirs_present_on_remote_server))


tasks = []
for dirname in localdirs_present_on_remote_server:
    tasks.append(Task(type=TaskType.LOCAL, dirname=dirname, base_dir=args.BASE_DIR))
    tasks.append(Task(type=TaskType.REMOTE, dirname=dirname, base_dir=args.REMOTE_DIR))
            

print(Fore.GREEN + f"Total of {len(tasks)} are planned for execution")


print(Fore.GREEN + "Starting to compute hashes")

results = joblib.Parallel(n_jobs=args.n)(joblib.delayed(process_task)(task) for task in tasks)
           
            
           
matching_dirs, nonmatching_dirs = match_hashes(results)


    
if len(nonmatching_dirs) == 0:
    if (len(matching_dirs) > 0) and (len(matching_dirs) == len(localdirs_present_on_remote_server)):
        print(Fore.GREEN + "All local directories are found remotely")

elif len(nonmatching_dirs) > 0:
    print(Fore.RED + f"{len(nonmatching_dirs)} are not complete on remote destination:")
    print("\n".join(nonmatching_dirs))


if len(matching_dirs) > 0:
    print(Fore.GREEN + "You can safely delete the following directories:")
    print("\n".join(matching_dirs))
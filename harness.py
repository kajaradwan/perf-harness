#This notebook functions elbencho test harness passing various parameters and thread counts to simulate workload patterns and help enumerate the 
#performance envelope of a vast system as it scales. 

#Prerequisites

#Install ElBencho on head node
#Install Elbencho on worker nodes
#Start Elbencho in services mode on worker nodes
#Install jupyter lab on head nodesatisfy any dependencies for python using pip
 
#The code below creates a number of test scenarios on a shared set of exports.  
#Each test will create a new target directory for each client so the workers do not overwrite each other.  
#Each test will create at least 4TB of test data so that we can ensure reads are fed from QLC rather than SCM.  
#The goal is to function the tests, generate graphical output, and observe at which point latency fell above 2ms.  
#This data will be helpful for JPMC to understand how the VAST system behaves and how the system scales.


import subprocess
import csv

# Define workload mixes, block sizes, and total capacities
workloads = {
    "Burn-In": {"block_size": "2M", "operation": "--write", "type": "write", "threads": [64], "total_size": "4T"},
    "1MB_seq_write": {"block_size": "1M", "operation": "--write", "type": "write", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "4T"},
    "1MB_seq_read": {"block_size": "1M", "operation": "--read", "type": "read", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "64k_seq_read": {"block_size": "64k", "operation": "--read", "type": "read", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "64k_rand_read": {"block_size": "64k", "operation": "--read --rand", "type": "read", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "4k_seq_read": {"block_size": "4k", "operation": "--read", "type": "read", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "4k_rand_read": {"block_size": "4k", "operation": "--read --rand", "type": "read", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "64k_seq_write": {"block_size": "64k", "operation": "--write", "type": "write", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "64k_rand_write": {"block_size": "64k", "operation": "--write --rand", "type": "write", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "4k_seq_write": {"block_size": "4k", "operation": "--write", "type": "write", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "4k_rand_write": {"block_size": "4k", "operation": "--write --rand", "type": "write", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "hpc": {"block_size": "32k", "operation": "--rwmixpct 75", "type": "mixed", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
    "sdw": {"block_size": "32k", "operation": "--rwmixpct 50", "type": "mixed", "threads": [1, 2, 4, 8, 16, 32, 64], "total_size": "1T"},
}

# Explicit list for Hosts to target
hosts = ["10.70.16.91", "10.70.16.93", "10.70.16.97", "10.70.16.101"]
#these all reference the same view!
base_mount_paths = [
    "/mnt/nfs1",
    "/mnt/nfs2",
    "/mnt/nfs3",
    "/mnt/nfs4",
    "/mnt/nfs5",
    "/mnt/nfs6",
    "/mnt/nfs7",
    "/mnt/nfs8"
]

# Track created directories for reuse during reads
created_directories = {}

# Function to execute a workload test
def run_elbencho_test(workload_name, workload_config): 
    threads = workload_config["threads"]
    block_size = workload_config["block_size"]
    operation = workload_config["operation"]
    test_type = workload_config["type"]
    total_size = workload_config["total_size"]
    
    # Convert total size from TB to GB for calculations
    total_size_gb = int(total_size[:-1]) * 1024

    # Loop over threads and run elbencho for each workload configuration
    for thread_count in threads:
        num_mounts = len(base_mount_paths)
        size_per_thread = (total_size_gb / (thread_count * num_mounts)) * 2  # Ensure full data is written

        file_size_per_thread = f"{int(size_per_thread)}G"  # Adjust file size per thread for each mount

        # Create shared directory paths across all hosts
        filepaths = []
        for base_mount in base_mount_paths:
            shared_dir = f"{base_mount}/{workload_name}_{thread_count}_threads"
            filepaths.append(shared_dir)
            
            # Only create new directories for write or mixed tests
            if test_type == "write" or test_type == "mixed":
                create_dir_command = f"clush -l vastdata -w {','.join(hosts)} 'mkdir -p {shared_dir}'"
                print(f"Creating shared test directory: {create_dir_command}")
                subprocess.run(create_dir_command, shell=True, check=True)

        # Track created directories for reuse in read tests
        if test_type == "write" or test_type == "mixed":
            created_directories[f"{workload_name}_{thread_count}_threads"] = filepaths
        elif test_type == "read":
            thread_dir_name = f"1MB_seq_write_{thread_count}_threads"
            filepaths = created_directories.get(thread_dir_name, [])
            if not filepaths:
                print(f"No valid directories found for {thread_dir_name}. Skipping read test.")
                continue

        # Prepare the positional arguments (file paths) for elbencho
        filepaths_str = " ".join(filepaths)
        command = (
            f"elbencho --threads {thread_count} --block {block_size} --direct --size {file_size_per_thread} {operation} "
            f"--hosts {','.join([f'{h}:1611' for h in hosts])} -d --lat --csvfile {workload_name}_{total_size}.csv {filepaths_str}"
        )
        print(f"Running command: {command}")

        # Execute the command
        try:
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running workload {workload_name} with {thread_count} threads: {e}")
            break

# Example usage to execute all workloads with custom capacities
def execute_all_workloads():
    for workload_name, workload_config in workloads.items():
        print(f"\n\nStarting workload: {workload_name}\n")
        run_elbencho_test(workload_name, workload_config)
        print(f"\nCompleted workload: {workload_name}\n")

# Run the workloads
execute_all_workloads()

import os

def concatenate_files(root_directory, output_filepath):
    """Concatenates all files in a directory and its subdirectories into one file."""
    if not root_directory or not output_filepath:
        raise ValueError("Root directory and output file path cannot be empty")
    if not os.path.exists(root_directory):
        raise FileNotFoundError(f"Root directory does not exist: {root_directory}")
    
    output_dir = os.path.dirname(output_filepath)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    with open(output_filepath, 'wb') as outfile:
        for dirpath, _, filenames in os.walk(root_directory):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                try:
                    with open(file_path, 'rb') as infile:
                        outfile.write(infile.read())
                except Exception as e:
                    print(f"Error reading file: {file_path} - {e}")

if __name__ == "__main__":
    default_root = "/data/data/com.termux/files/home/storage/shared/py/sim"
    default_output = "/data/data/com.termux/files/home/storage/shared/py/sim/util/conc.txt"
    
    print(f"Enter root directory path (default: {default_root}):")
    root_directory = input().strip() or default_root
    print(f"Enter output file path (default: {default_output}):")
    output_filepath = input().strip() or default_output

    try:
        concatenate_files(root_directory, output_filepath)
        print(f"Files concatenated to: {output_filepath}")
    except Exception as e:
        print(f"Error: {e}")
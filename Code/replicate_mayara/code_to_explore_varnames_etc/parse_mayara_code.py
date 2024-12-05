import os
import glob

def collect_files(root_directory):
    """
    Walk through the directory tree and collect all `.do` and `.sas` file paths.
    """
    file_paths = []
    for current_directory, subdirs, files in os.walk(root_directory):
        for extension in ["*.do", "*.sas"]:
            file_paths.extend(glob.glob(os.path.join(current_directory, extension)))
    return sorted(file_paths)  # Sort all collected file paths alphabetically

def create_combined_file(file_paths, output_file):
    """
    Create a combined file containing all the contents of the provided file paths.
    Each file's content is prefixed with a header containing its absolute path.
    """
    with open(output_file, "w") as outfile:
        for file_path in file_paths:
            print(f"Adding file: {file_path}")
            try:
                with open(file_path, "r") as infile:
                    content = infile.read()
                    # Add a header with the absolute file path
                    header = f"--- START OF {os.path.abspath(file_path)} ---\n"
                    footer = f"\n--- END OF {os.path.abspath(file_path)} ---\n\n"
                    outfile.write(header)
                    outfile.write(content)
                    outfile.write(footer)
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")

def main(root_directory):
    """
    Main function to generate combined code files.
    """
    # Collect all `.do` and `.sas` file paths
    file_paths = collect_files(root_directory)
    
    # Generate a combined file for all `.do` and `.sas` files
    combined_all_file = os.path.join(root_directory, "combined_code_all.txt")
    create_combined_file(file_paths, combined_all_file)

    # Generate individual combined files for each parent directory
    parent_dirs = {}
    for file_path in file_paths:
        parent_dir = os.path.dirname(file_path)
        if parent_dir not in parent_dirs:
            parent_dirs[parent_dir] = []
        parent_dirs[parent_dir].append(file_path)
    
    for parent_dir, files in parent_dirs.items():
        relative_path = os.path.relpath(parent_dir, root_directory).replace(os.sep, "_")
        output_file = os.path.join(root_directory, f"combined_code_{relative_path}.txt")
        create_combined_file(files, output_file)

    print("\nAll combined files created successfully!")

# Replace this with your actual root directory path
root_directory = "/Users/jfogel/Downloads/Felix_JMP"
main(root_directory)

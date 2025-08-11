import os
import shutil

# === USER CONFIGURATION ===
SOURCE_DIR = r"C:\Users\kirth\OneDrive\Pictures\iphone15_April2025"
DESTINATION_DIR = r"C:\Users\kirth\OneDrive\Pictures\iphone15_April2025\CopyMaster"
# ===========================

def move_files_with_dash_one(src_dir, dest_dir):
    if not os.path.isdir(src_dir):
        print(f"Source directory does not exist: {src_dir}")
        return
    if not os.path.isdir(dest_dir):
        print(f"Destination directory does not exist. Creating it: {dest_dir}")
        os.makedirs(dest_dir)

    all_files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
    total = len(all_files)
    moved = 0

    for i, filename in enumerate(all_files, start=1):
        src_path = os.path.join(src_dir, filename)
        dest_path = os.path.join(dest_dir, filename)

        if "- 1" in filename:
            shutil.move(src_path, dest_path)
            moved += 1
            status = "moved"
        else:
            status = "skipped"

        print(f"Processing file {i} of {total}: {filename} -> {status}")

    print(f"\nFinished. Total files processed: {total}, Files moved: {moved}, Skipped: {total - moved}")

if __name__ == "__main__":
    move_files_with_dash_one(SOURCE_DIR, DESTINATION_DIR)
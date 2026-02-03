from intrafact.config import RAW_DATA_DIR

def ingestor():
    """
    Reads all files from the raw data directory and returns a list 
    of data objects for the normalization layer.
    """
    # 1. Check if the directory exists
    if not RAW_DATA_DIR.exists():
        print(f"Directory not found: {RAW_DATA_DIR}")
        return []
    
    # 2. Get all files in the directory
    files = list(RAW_DATA_DIR.glob("*"))
    collected_data = []

    for file_path in files:
        # 3. Process only files (skip hidden folders or sub-directories)
        if file_path.is_file():
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    
                    # 4. Create a standardized dictionary for this file
                    data_item = {
                        "raw_text": content,
                        "metadata": {
                            "file_name": file_path.name,
                            "source_type": "local_file"
                        }
                    }
                    collected_data.append(data_item)
            except Exception as e:
                print(f"Failed to read {file_path.name}: {e}")

    # 5. Return the list of all collected data
    return collected_data

if __name__ == "__main__":
    # For local testing, we still print to see if it works
    ingestor()

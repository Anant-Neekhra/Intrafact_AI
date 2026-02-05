import sys
from intrafact.ingestion.file_ingestor import ingestor
from intrafact.normalization.normalizer import TextNormalizer

def run_pipeline():
    print("-----Starting Pipeline------")

    print("-----Starting Ingestion------")

    raw_data = ingestor()

    if not raw_data:
        print("No raw data found")
        return
    
    print(f"Found {len(raw_data)} files in raw data")

    print("-----Starting Normalisation-----")
    
    norm = TextNormalizer()

    for file in raw_data:
        original_file_name = file["metadata"]["file_name"]
        print(f"Normalising {original_file_name}")

        try:
            processed_data = norm.normalize(
                file["raw_text"], 
                file["metadata"]
                )
            
            save_path = norm.save_object(
                processed_data, 
                original_file_name
                )
            
            print(f"Done! saved to {save_path.name}")

        except Exception as e:
            print(f"Error {e}")

    print("-----Pipeline Complete------")

if __name__== "__main__":
    run_pipeline()
           
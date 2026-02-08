import sys
from intrafact.ingestion.file_ingestor import ingestor
from intrafact.normalization.normalizer import TextNormalizer
from intrafact.processing.chunker import TextChunker 
from intrafact.processing.embedder import TextEmbedder # <-- New Import

def run_pipeline():
    print("-----Starting Pipeline------")


    #Step 1 Ingestion
    print("-----Starting Ingestion------")

    raw_data = ingestor()

    if not raw_data:
        print("No raw data found")
        return
    
    print(f"Found {len(raw_data)} files in raw data")

    #Step 2 processing
    print("-----Starting Normalisation-----")
    
    norm = TextNormalizer()
    chunker = TextChunker()
    embedder = TextEmbedder()

    for file in raw_data:
        original_file_name = file["metadata"]["file_name"]
        print(f"Normalising {original_file_name}")

        try:
            normalized_data = norm.normalize(
                file['raw_text'], 
                file['metadata']
            )
            
            chunks = chunker.process_chunks(normalized_data)
            print(f"   ↳ Split into {len(chunks)} chunks.")

            chunks_with_vectors = embedder.embed_chunks(chunks)
            print(f"   ↳ Generated {len(chunks_with_vectors[0]['embedding'])}-dimension vectors.")

            for i, chunk_obj in enumerate(chunks):
                save_path = norm.save_object(
                    chunk_obj, 
                    original_file_name
                )
                print(f"      [Chunk {i+1}] Saved to: {save_path.name}")
            
            print("   ✅ Processing Complete!")

        except Exception as e:
            print(f"❌ Error processing {original_file_name}: {e}")

    print("-----Pipeline Complete------")

if __name__== "__main__":
    run_pipeline()
           
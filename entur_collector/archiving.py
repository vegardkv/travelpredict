import zipfile
from datetime import datetime
from pathlib import Path
from .config import PROCESSED_DIR

def archive_processed_data():
    print("Starting archive_processed_data function...")
    source_path = Path(PROCESSED_DIR)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_zip = source_path / f"archive_{timestamp}.zip"
    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for json_file in source_path.glob("*.json"):
            print(f"Archiving {json_file.name}")
            zf.write(str(json_file), arcname=json_file.name)
    for json_file in source_path.glob("*.json"):
        print(f"Removing {json_file.name}")
        json_file.unlink()

    print(f"Archive complete: {output_zip}")

if __name__ == "__main__":
    archive_processed_data()
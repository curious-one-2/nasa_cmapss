import shutil

def move_ingested_source_file(source_file_path : str, archived_file_path : str):
            # 4. Archive the source file
        try:    
            shutil.move(str(source_file_path), str(archived_file_path))
            return "ARCHIVE_SUCCESS"
        except Exception:
            return "ARCHIVE_ERROR"
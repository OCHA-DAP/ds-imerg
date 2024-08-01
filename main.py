from src.datasources.imerg import create_auth_files, download_recent_imerg

if __name__ == "__main__":
    create_auth_files()
    download_recent_imerg()

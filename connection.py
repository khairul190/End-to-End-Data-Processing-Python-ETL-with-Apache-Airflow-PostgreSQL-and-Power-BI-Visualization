from sqlalchemy import create_engine

# Ganti parameter koneksi sesuai dengan konfigurasi PostgreSQL Anda
def postgresql_con():
    db_connection_str = 'postgresql://postgres:12345678@host.docker.internal:5432/postgres'
    db_connection = create_engine(db_connection_str)

    try:
        # Coba menjalankan kueri sederhana, misalnya SELECT 1
        db_connection.execute("SELECT 1")
        print("Koneksi ke PostgreSQL berhasil.")
    except Exception as e:
        print(f"Koneksi ke PostgreSQL gagal. Error: {e}")
    
    return db_connection
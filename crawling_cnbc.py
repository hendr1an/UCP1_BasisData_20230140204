import requests
from bs4 import BeautifulSoup
import certifi
from pymongo import MongoClient
from datetime import datetime

def job_crawling_cnbc():
    print("Memulai proses crawling (Jalur Bypass Halaman Tag Lingkungan)...")
    
    # --- PENTING: GANTI PASSWORD KAMU DI SINI ---
    uri = "mongodb+srv://hendriansyahgyan_db_user:gyan1234@cluster0.b2cilcn.mongodb.net/?appName=Cluster0"
    
    try:
        client = MongoClient(uri, tlsCAFile=certifi.where())
        db = client['ujian_cloud_database'] 
        collection = db['cnbc_sustainability']
        collection.delete_many({}) # Kosongkan data lama
        print("Koneksi ke MongoDB Sukses.")
    except Exception as e:
        print("Gagal koneksi ke MongoDB:", e)
        return
        
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    # KITA PERBANYAK SUMBERNYA BIAR DAPAT BANYAK LINK
    target_urls = [
        "https://www.cnbcindonesia.com/tag/lingkungan",
        "https://www.cnbcindonesia.com/tag/sustainability",
        "https://www.cnbcindonesia.com/tag/perubahan-iklim"
    ]
    
    valid_links = []
    
    for url in target_urls:
        try:
            res = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(res.text, 'html.parser')
            
            # Cari semua link di halaman tersebut
            for a in soup.find_all('a', href=True):
                link = a['href']
                # Pastikan ini link artikel berita sungguhan
                if 'cnbcindonesia.com/news/' in link or 'cnbcindonesia.com/tech/' in link or 'cnbcindonesia.com/market/' in link:
                    if '/tv/' not in link and '/foto/' not in link and link not in valid_links:
                        valid_links.append(link)
        except:
            pass
                    
    print(f"Dapat {len(valid_links)} link berita. Mengekstrak detail...")
    
    data_berita = []
    count = 0
    
    for link in valid_links:
        if count >= 7: 
            break
            
        try:
            detail_res = requests.get(link, headers=headers, timeout=15)
            
            # Jika akses diblokir oleh Cloudflare di halaman detail
            if detail_res.status_code != 200:
                continue
                
            detail_soup = BeautifulSoup(detail_res.text, 'html.parser')
            
            # Ekstrak elemen dasar dengan sangat aman menggunakan meta tag
            judul_tag = detail_soup.find('meta', attrs={'property': 'og:title'})
            judul = judul_tag['content'] if judul_tag else "Tanpa Judul"
            
            date_tag = detail_soup.find('meta', attrs={'name': 'publishdate'})
            tanggal = date_tag['content'] if date_tag else "Tanpa Tanggal"
            
            author_tag = detail_soup.find('meta', attrs={'name': 'author'})
            author = author_tag['content'] if author_tag else "Tim CNBC"
            
            # JURUS PAMUNGKAS: Ambil semua teks paragraf tanpa pandang bulu
            paragraphs = detail_soup.find_all('p')
            isi_berita = ' '.join([p.get_text(strip=True) for p in paragraphs])
            
            thumb_tag = detail_soup.find('meta', attrs={'property': 'og:image'})
            thumbnail = thumb_tag['content'] if thumb_tag else "Tidak ada"
                
            # Pastikan isi berita cukup panjang (bukan sekadar halaman error)
            if len(isi_berita) > 100 and judul != "Tanpa Judul":
                data_berita.append({
                    "url": link,
                    "judul": judul,
                    "tanggal_publish": tanggal,
                    "author": author,
                    "tag_kategori": ["Lingkungan", "Sustainability"],
                    "isi_berita": isi_berita,
                    "thumbnail": thumbnail,
                    "crawled_at": datetime.now()
                })
                count += 1
                print(f"{count}. SUKSES: {judul[:50]}...")
                
        except Exception as e:
            continue 
            
    # Simpan ke Database
    if data_berita:
        collection.insert_many(data_berita)
        print(f"\nUjian Selesai! {len(data_berita)} data lingkungan berhasil masuk ke MongoDB.")
    else:
        print("\nGagal mengekstrak isi konten. Akses mungkin sedang dicegat Cloudflare sementara.")

if __name__ == "__main__":
    job_crawling_cnbc()
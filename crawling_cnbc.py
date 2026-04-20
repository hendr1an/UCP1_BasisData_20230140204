from curl_cffi import requests 
from bs4 import BeautifulSoup
import certifi
from pymongo import MongoClient
from datetime import datetime

def job_crawling_cnbc():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Memulai proses crawling CNBC (Mode Sapu Bersih)...")
    
    # --- PENTING: GANTI PASSWORD_KAMU_YANG_BENAR DENGAN PASSWORD ASLIMU ---
    uri = "mongodb+srv://hendriansyahgyan_db_user:gyan1234@cluster0.b2cilcn.mongodb.net/?appName=Cluster0"
    
    try:
        client = MongoClient(uri, tlsCAFile=certifi.where())
        db = client['ujian_cloud_database'] 
        collection = db['cnbc_sustainability']
        collection.delete_many({}) # Kosongkan data lama
        print("Koneksi ke MongoDB Aman.")
    except Exception as e:
        print(f"Gagal konek ke MongoDB: {e}")
        return
    
    target_urls = [
        "https://www.cnbcindonesia.com/search?query=sustainability",
        "https://www.cnbcindonesia.com/search?query=lingkungan",
        "https://www.cnbcindonesia.com/news/indeks"
    ]
    
    valid_links = []
    
    try:
        for search_url in target_urls:
            if len(valid_links) >= 15:
                break
                
            res = requests.get(search_url, impersonate="chrome110", timeout=15)
            soup = BeautifulSoup(res.text, 'html.parser')
            all_links = soup.find_all('a', href=True)
            
            for a in all_links:
                link = a['href']
                if 'cnbcindonesia.com' in link and '/search' not in link and '/tv/' not in link and '/tag/' not in link and link not in valid_links:
                    valid_links.append(link)
                    
        data_berita = []
        count = 0
        
        print(f"BINGOO! Menemukan {len(valid_links)} kandidat link. Mulai mengekstrak data...")
        
        for link in valid_links:
            if count >= 7: 
                break
                
            try:
                detail_res = requests.get(link, impersonate="chrome110", timeout=15)
                detail_soup = BeautifulSoup(detail_res.text, 'html.parser')
                
                url = link
                
                # Mengambil Judul (Lebih fleksibel)
                judul_tag = detail_soup.find('meta', attrs={'property': 'og:title'})
                if not judul_tag:
                    judul_tag = detail_soup.find('title')
                judul = judul_tag['content'] if judul_tag and judul_tag.has_attr('content') else (judul_tag.text if judul_tag else "Tanpa Judul")
                
                tanggal_tag = detail_soup.find('meta', attrs={'name': 'publishdate'})
                tanggal = tanggal_tag['content'] if tanggal_tag else "Tanpa Tanggal"
                
                author_tag = detail_soup.find('meta', attrs={'name': 'author'})
                author = author_tag['content'] if author_tag else "Tim CNBC"
                
                tags_elements = detail_soup.find_all('a', class_='gtag')
                tag_kategori = [t.text.strip() for t in tags_elements] if tags_elements else []
                
                # Mengambil Isi (Jika detail_text tidak ada, ambil apa saja yang berbentuk paragraf)
                body = detail_soup.find('div', class_='detail_text')
                if body:
                    isi_berita = ' '.join([p.get_text(strip=True) for p in body.find_all('p')])
                else:
                    article_tag = detail_soup.find('article')
                    if article_tag:
                        isi_berita = ' '.join([p.get_text(strip=True) for p in article_tag.find_all('p')])
                    else:
                        isi_berita = "Konten ada, namun tidak bisa terbaca teksnya."
                    
                thumbnail_tag = detail_soup.find('meta', attrs={'property': 'og:image'})
                thumbnail = thumbnail_tag['content'] if thumbnail_tag else "Tanpa Thumbnail"
                
                # MODE SAPU BERSIH: Simpan asalkan ada judulnya!
                if judul != "Tanpa Judul":
                    data_berita.append({
                        "url": url,
                        "judul": judul.strip(),
                        "tanggal_publish": tanggal,
                        "author": author,
                        "tag_kategori": tag_kategori,
                        "isi_berita": isi_berita,
                        "thumbnail": thumbnail,
                        "crawled_at": datetime.now()
                    })
                    count += 1
                    print(f"{count}. SUKSES: {judul[:50]}...")
                    
            except Exception as e:
                print(f"Gagal memproses satu link, lanjut ke link berikutnya...")
                continue 
                
        if data_berita:
            collection.insert_many(data_berita)
            print(f"\nSUKSES BESAR! {len(data_berita)} Data berita dari CNBC berhasil dijebol dan masuk ke MongoDB!")
        else:
            print("\nGagal mengekstrak konten berita.")
            
    except Exception as e:
        print(f"Terjadi kesalahan sistem: {e}")

if __name__ == "__main__":
    job_crawling_cnbc()
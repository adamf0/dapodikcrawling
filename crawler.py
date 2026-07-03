import asyncio
import os
import datetime
import re
import httpx
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Province, Kabupaten, Kecamatan, Sekolah, CrawlJob, CrawledKecamatan

# Global set for cancelled jobs
CANCELLED_JOBS = set()

# Global lock for updating CrawlJob table in SQLite to prevent concurrent lock errors
DB_LOCK = asyncio.Lock()

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

class JobLogger:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.file_path = f"logs/{job_id}.log"
        with open(self.file_path, "w", encoding="utf-8") as f:
            f.write(f"[{datetime.datetime.now()}] Job {job_id} initialized.\n")

    def log(self, message: str):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"[{timestamp}] {message}\n"
        with open(self.file_path, "a", encoding="utf-8") as f:
            f.write(log_line)
        print(f"[Job-{self.job_id}] {message}")


async def update_job_db(job_id: str, field_updates: dict):
    """Safely updates CrawlJob record using a lock to avoid SQLite database locks."""
    async with DB_LOCK:
        db: Session = SessionLocal()
        try:
            job = db.query(CrawlJob).filter(CrawlJob.id == job_id).first()
            if job:
                for key, value in field_updates.items():
                    if key.startswith('increment_'):
                        real_key = key.replace('increment_', '')
                        current_val = getattr(job, real_key) or 0
                        setattr(job, real_key, current_val + value)
                    else:
                        setattr(job, key, value)
                db.commit()
        except Exception as e:
            print(f"Error updating job progress: {e}")
        finally:
            db.close()


async def fetch_json_with_retry(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
    logger: JobLogger,
    delay: float = 0.5,
    max_retries: int = 10,
    backoff_factor: float = 2.0
) -> list:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
        "Referer": "https://dapo.kemendikdasmen.go.id/",
    }
    
    async with semaphore:
        for attempt in range(1, max_retries + 1):
            try:
                response = await client.get(url, headers=headers, timeout=30.0)
                if response.status_code == 200:
                    # Polite delay after successful fetch
                    if delay > 0:
                        await asyncio.sleep(delay)
                    return response.json()
                elif response.status_code == 503:
                    sleep_time = max(8.0, backoff_factor ** attempt * 4.0)
                    logger.log(f"[RATE-LIMIT] Nginx 503 Rate Limited on {url}. Backing off for {sleep_time}s... (Attempt {attempt}/{max_retries})")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.log(f"HTTP Error {response.status_code} for URL: {url} (Attempt {attempt}/{max_retries})")
            except Exception as e:
                logger.log(f"Request Exception: {str(e)} for URL: {url} (Attempt {attempt}/{max_retries})")
            
            if attempt < max_retries:
                sleep_time = backoff_factor ** attempt
                await asyncio.sleep(sleep_time)
                
        raise Exception(f"Failed to fetch JSON from {url} after {max_retries} attempts.")


async def fetch_html_with_retry(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
    logger: JobLogger,
    delay: float = 0.5,
    max_retries: int = 10,
    backoff_factor: float = 2.0
) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
        "Referer": "https://dapo.kemendikdasmen.go.id/",
    }
    
    async with semaphore:
        for attempt in range(1, max_retries + 1):
            try:
                response = await client.get(url, headers=headers, timeout=20.0)
                if response.status_code == 200:
                    # Polite delay after successful fetch
                    if delay > 0:
                        await asyncio.sleep(delay)
                    return response.text
                elif response.status_code == 503:
                    sleep_time = max(8.0, backoff_factor ** attempt * 4.0)
                    logger.log(f"[RATE-LIMIT] Nginx 503 Rate Limited on {url}. Backing off for {sleep_time}s... (Attempt {attempt}/{max_retries})")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.log(f"HTTP Error {response.status_code} for URL: {url} (Attempt {attempt}/{max_retries})")
            except Exception as e:
                logger.log(f"Request Exception: {str(e)} for URL: {url} (Attempt {attempt}/{max_retries})")
            
            if attempt < max_retries:
                sleep_time = backoff_factor ** attempt
                await asyncio.sleep(sleep_time)
                
        raise Exception(f"Failed to fetch HTML from {url} after {max_retries} attempts.")


# ----------------------------------------------------
# SCRAPING WORKER & PARSER
# ----------------------------------------------------

async def fetch_school_details(
    client: httpx.AsyncClient,
    school_id_enkrip: str,
    semaphore: asyncio.Semaphore,
    delay: float,
    logger: JobLogger
) -> dict:
    details = {
        "status_kepemilikan": None,
        "sk_pendirian_sekolah": None,
        "tanggal_sk_pendirian": None,
        "sk_izin_operasional": None,
        "tanggal_sk_izin_operasional": None,
        "desa_kelurahan": None,
        "kecamatan_scraped": None,
        "kabupaten_scraped": None,
        "provinsi_scraped": None,
        "kode_pos": None,
        "kepsek": None,
        "operator": None,
        "akreditasi": None,
        "kurikulum": None,
        "waktu": None
    }
    
    if not school_id_enkrip:
        return details
        
    url = f"https://dapo.kemendikdasmen.go.id/sekolah/{school_id_enkrip}"
    try:
        html = await fetch_html_with_retry(client, url, semaphore, logger, delay)
        
        # Parse Identitas
        identitas = re.findall(r'<p>\s*<strong>\s*(.*?)\s*:\s*</strong>\s*(.*?)\s*</p>', html)
        identitas_dict = {key.strip(): val.strip() for key, val in identitas}
        
        details["status_kepemilikan"] = identitas_dict.get("Status Kepemilikan")
        details["sk_pendirian_sekolah"] = identitas_dict.get("SK Pendirian Sekolah")
        details["tanggal_sk_pendirian"] = identitas_dict.get("Tanggal SK Pendirian")
        details["sk_izin_operasional"] = identitas_dict.get("SK Izin Operasional")
        details["tanggal_sk_izin_operasional"] = identitas_dict.get("Tanggal SK Izin Operasional")
        details["desa_kelurahan"] = identitas_dict.get("Desa / Kelurahan")
        details["kecamatan_scraped"] = identitas_dict.get("Kecamatan")
        details["kabupaten_scraped"] = identitas_dict.get("Kabupaten")
        details["provinsi_scraped"] = identitas_dict.get("Provinsi")
        details["kode_pos"] = identitas_dict.get("Kode Pos")
        
        # Parse Usermenu
        usermenu = re.findall(r'([A-Za-z]+)\s*:\s*<strong>\s*(.*?)\s*</strong>', html)
        usermenu_dict = {key.strip(): val.strip() for key, val in usermenu}
        
        details["kepsek"] = usermenu_dict.get("Kepsek")
        details["operator"] = usermenu_dict.get("Operator")
        details["akreditasi"] = usermenu_dict.get("Akreditasi")
        details["kurikulum"] = usermenu_dict.get("Kurikulum")
        details["waktu"] = usermenu_dict.get("Waktu")
    except Exception:
        pass
        
    return details


# ----------------------------------------------------
# CHAINED REQUEST WORKERS
# ----------------------------------------------------

async def chain_crawl_kecamatan(
    client: httpx.AsyncClient,
    kc_code: str,
    kc_name: str,
    bentuk: str,
    semester_id: str,
    semaphore: asyncio.Semaphore,
    delay: float,
    force_recrawl: bool,
    job_id: str,
    logger: JobLogger
):
    if job_id in CANCELLED_JOBS:
        return
    
    # Check cache unless force_recrawl
    if not force_recrawl:
        db = SessionLocal()
        crawled = db.query(CrawledKecamatan).filter(
            CrawledKecamatan.kecamatan_id == kc_code,
            CrawledKecamatan.bentuk_pendidikan == bentuk
        ).first()
        db.close()
        if crawled:
            logger.log(f"Cache Hit: Skipping {bentuk.upper()} schools for Kecamatan: {kc_name}")
            await update_job_db(job_id, {"increment_processed_sekolahs": 1})
            return

    logger.log(f"Crawl: Fetching {bentuk.upper()} schools for Kecamatan: {kc_name} ({kc_code})...")
    school_url = f"https://dapo.kemendikdasmen.go.id/rekap/progresSP?id_level_wilayah=3&kode_wilayah={kc_code}&semester_id={semester_id}&bentuk_pendidikan_id={bentuk}"
    
    try:
        school_data = await fetch_json_with_retry(client, school_url, semaphore, logger, delay)
        db_local = SessionLocal()
        
        # Resolve all school profiles in parallel
        async def process_single_school(item):
            s_id = item.get("sekolah_id", "").strip()
            s_enkrip = item.get("sekolah_id_enkrip", "").strip()
            s_name = item.get("nama", "").strip()
            s_npsn = str(item.get("npsn", "")).strip()
            s_bentuk = item.get("bentuk_pendidikan", "").strip()
            s_status = item.get("status_sekolah", "").strip()
            
            ptk = int(item.get("ptk", 0))
            peg = int(item.get("pegawai", 0))
            pd = int(item.get("pd", 0))
            rombel = int(item.get("rombel", 0))
            rk = int(item.get("jml_rk", 0))
            lab = int(item.get("jml_lab", 0))
            perpus = int(item.get("jml_perpus", 0))
            sinkron = item.get("sinkron_terakhir", "").strip()
            
            # Check cache first in SQLite before fetching profile page from web
            details = None
            if not force_recrawl:
                existing = db_local.query(Sekolah).filter(Sekolah.sekolah_id == s_id).first()
                if existing and existing.status_kepemilikan is not None:
                    details = {
                        "status_kepemilikan": existing.status_kepemilikan,
                        "sk_pendirian_sekolah": existing.sk_pendirian_sekolah,
                        "tanggal_sk_pendirian": existing.tanggal_sk_pendirian,
                        "sk_izin_operasional": existing.sk_izin_operasional,
                        "tanggal_sk_izin_operasional": existing.tanggal_sk_izin_operasional,
                        "desa_kelurahan": existing.desa_kelurahan,
                        "kecamatan_scraped": existing.kecamatan_scraped,
                        "kabupaten_scraped": existing.kabupaten_scraped,
                        "provinsi_scraped": existing.provinsi_scraped,
                        "kode_pos": existing.kode_pos,
                        "kepsek": existing.kepsek,
                        "operator": existing.operator,
                        "akreditasi": existing.akreditasi,
                        "kurikulum": existing.kurikulum,
                        "waktu": existing.waktu
                    }
                    
            if not details:
                logger.log(f"Scraping profile: {s_name} ({s_npsn})...")
                details = await fetch_school_details(client, s_enkrip, semaphore, delay, logger)
            else:
                logger.log(f"Cache Hit (DB): Skipping profile scrape for {s_name} ({s_npsn})")
            
            db_local.merge(Sekolah(
                sekolah_id=s_id,
                sekolah_id_enkrip=s_enkrip,
                nama=s_name,
                npsn=s_npsn,
                bentuk_pendidikan=s_bentuk,
                status_sekolah=s_status,
                ptk=ptk,
                pegawai=peg,
                pd=pd,
                rombel=rombel,
                jml_rk=rk,
                jml_lab=lab,
                jml_perpus=perpus,
                sinkron_terakhir=sinkron,
                status_kepemilikan=details["status_kepemilikan"],
                sk_pendirian_sekolah=details["sk_pendirian_sekolah"],
                tanggal_sk_pendirian=details["tanggal_sk_pendirian"],
                sk_izin_operasional=details["sk_izin_operasional"],
                tanggal_sk_izin_operasional=details["tanggal_sk_izin_operasional"],
                desa_kelurahan=details["desa_kelurahan"],
                kecamatan_scraped=details["kecamatan_scraped"],
                kabupaten_scraped=details["kabupaten_scraped"],
                provinsi_scraped=details["provinsi_scraped"],
                kode_pos=details["kode_pos"],
                kepsek=details["kepsek"],
                operator=details["operator"],
                akreditasi=details["akreditasi"],
                kurikulum=details["kurikulum"],
                waktu=details["waktu"],
                kecamatan_id=kc_code
            ))
            
        # Concurrently resolve and write schools
        await asyncio.gather(*(process_single_school(item) for item in school_data))
        
        # Save cache
        db_local.merge(CrawledKecamatan(kecamatan_id=kc_code, bentuk_pendidikan=bentuk))
        db_local.commit()
        db_local.close()
    except Exception as e:
        logger.log(f"Error fetching {bentuk} schools for {kc_name}: {e}")
        
    await update_job_db(job_id, {"increment_processed_sekolahs": 1})


async def chain_crawl_kabupaten(
    client: httpx.AsyncClient,
    k_code: str,
    k_name: str,
    bentuk_pendidikan_list: list,
    semester_id: str,
    semaphore: asyncio.Semaphore,
    delay: float,
    force_recrawl: bool,
    job_id: str,
    logger: JobLogger
):
    if job_id in CANCELLED_JOBS:
        return
        
    db_local = SessionLocal()
    existing_kecs = []
    if not force_recrawl:
        existing_kecs = db_local.query(Kecamatan).filter(Kecamatan.mst_kode_wilayah == k_code).all()
        
    kec_items = []
    try:
        if existing_kecs:
            logger.log(f"Cache Hit (DB): Found {len(existing_kecs)} Kecamatans for Kabupaten {k_name} in DB.")
            for kec in existing_kecs:
                kec_items.append((kec.kode_wilayah, kec.nama))
            db_local.close()
            kec_data = [] # Mock for increment processed count later
        else:
            db_local.close()
            logger.log(f"Crawl: Fetching Kecamatans for Kabupaten: {k_name} ({k_code})...")
            kec_data = await fetch_json_with_retry(client, kec_url, semaphore, logger, delay)
            db_local = SessionLocal()
            for item in kec_data:
                kc_code = item.get("kode_wilayah", "").strip()
                kc_name = item.get("nama", "").strip()
                kc_mst = item.get("mst_kode_wilayah", "").strip()
                
                db_local.merge(Kecamatan(kode_wilayah=kc_code, nama=kc_name, mst_kode_wilayah=kc_mst))
                kec_items.append((kc_code, kc_name))
            db_local.commit()
            db_local.close()
        
        # Increment totals in database
        total_sekolahs_added = len(kec_items) * len(bentuk_pendidikan_list)
        await update_job_db(job_id, {
            "increment_total_kecamatans": len(kec_items),
            "increment_total_sekolahs": total_sekolahs_added
        })
        
        # Crawl all districts (kecamatans) sequentially to avoid interleaving queue delays
        for kc_code, kc_name in kec_items:
            for bentuk in bentuk_pendidikan_list:
                if job_id in CANCELLED_JOBS:
                    return
                await chain_crawl_kecamatan(
                    client, kc_code, kc_name, bentuk, semester_id,
                    semaphore, delay, force_recrawl, job_id, logger
                )
        
    except Exception as e:
        logger.log(f"Error fetching Kecamatans for {k_name}: {e}")
        
    await update_job_db(job_id, {
        "increment_processed_kabupatens": 1,
        "increment_processed_kecamatans": len(kec_data) if 'kec_data' in locals() else 0
    })


async def chain_crawl_province(
    client: httpx.AsyncClient,
    prov_code: str,
    prov_name: str,
    bentuk_pendidikan_list: list,
    semester_id: str,
    semaphore: asyncio.Semaphore,
    delay: float,
    force_recrawl: bool,
    job_id: str,
    logger: JobLogger
):
    if job_id in CANCELLED_JOBS:
        return
        
    db_local = SessionLocal()
    existing_kabs = []
    if not force_recrawl:
        existing_kabs = db_local.query(Kabupaten).filter(Kabupaten.mst_kode_wilayah == prov_code).all()
        
    kab_items = []
    try:
        if existing_kabs:
            logger.log(f"Cache Hit (DB): Found {len(existing_kabs)} Kabupatens for Province {prov_name} in DB.")
            for kab in existing_kabs:
                kab_items.append((kab.kode_wilayah, kab.nama))
            db_local.close()
        else:
            db_local.close()
            logger.log(f"Crawl: Fetching Kabupatens for Province: {prov_name} ({prov_code})...")
            kab_data = await fetch_json_with_retry(client, kab_url, semaphore, logger, delay)
            db_local = SessionLocal()
            for item in kab_data:
                k_code = item.get("kode_wilayah", "").strip()
                k_name = item.get("nama", "").strip()
                k_mst = item.get("mst_kode_wilayah", "").strip()
                
                db_local.merge(Kabupaten(kode_wilayah=k_code, nama=k_name, mst_kode_wilayah=k_mst))
                kab_items.append((k_code, k_name))
            db_local.commit()
            db_local.close()
        
        # Increment total kabupatens dynamically
        await update_job_db(job_id, {"increment_total_kabupatens": len(kab_items)})
        
        # Crawl all regencies (kabupatens) sequentially
        for k_code, k_name in kab_items:
            if job_id in CANCELLED_JOBS:
                return
            await chain_crawl_kabupaten(
                client, k_code, k_name, bentuk_pendidikan_list, semester_id,
                semaphore, delay, force_recrawl, job_id, logger
            )
        
    except Exception as e:
        logger.log(f"Error fetching Kabupatens for Province {prov_name}: {e}")
        
    await update_job_db(job_id, {"increment_processed_provinces": 1})


async def crawl_dapodik_task(
    job_id: str,
    target_provinsi_ids: list = None,
    bentuk_pendidikan_list: list = None,
    semester_id: str = "20252",
    concurrency_limit: int = 5,
    delay: float = 0.5,
    force_recrawl: bool = False
):
    logger = JobLogger(job_id)
    logger.log("Starting Dapodik Chained Request crawler with polite throttling...")
    
    if not bentuk_pendidikan_list:
        bentuk_pendidikan_list = ["sd", "smp", "sma"]
        
    logger.log(f"Forms target: {bentuk_pendidikan_list}")
    logger.log(f"Semester ID: {semester_id}")
    logger.log(f"Concurrency limit: {concurrency_limit} requests")
    logger.log(f"Polite request delay: {delay} seconds")
    
    db: Session = SessionLocal()
    job = db.query(CrawlJob).filter(CrawlJob.id == job_id).first()
    if not job:
        logger.log("Job record not found. Exiting.")
        db.close()
        return

    # Initialize job counters
    job.status = "running"
    job.current_step = "provinces"
    job.total_provinces = 0
    job.processed_provinces = 0
    job.total_kabupatens = 0
    job.processed_kabupatens = 0
    job.total_kecamatans = 0
    job.processed_kecamatans = 0
    job.total_sekolahs = 0
    job.processed_sekolahs = 0
    db.commit()
    db.close()
    
    semaphore = asyncio.Semaphore(concurrency_limit)
    limits = httpx.Limits(max_keepalive_connections=concurrency_limit, max_connections=concurrency_limit * 2)
    
    async with httpx.AsyncClient(limits=limits) as client:
        try:
            if job_id in CANCELLED_JOBS:
                raise asyncio.CancelledError()
                
            db_local = SessionLocal()
            existing_provs = []
            if not force_recrawl:
                existing_provs = db_local.query(Province).all()
                
            provinces_to_process = []
            if existing_provs:
                logger.log(f"Cache Hit (DB): Found {len(existing_provs)} Provinces in DB.")
                for prov in existing_provs:
                    if target_provinsi_ids and prov.kode_wilayah not in target_provinsi_ids:
                        continue
                    provinces_to_process.append((prov.kode_wilayah, prov.nama))
                db_local.close()
            else:
                db_local.close()
                logger.log("Fetching Provinces from Root...")
                prov_url = f"https://dapo.kemendikdasmen.go.id/rekap/dataSekolah?id_level_wilayah=0&kode_wilayah=000000&semester_id={semester_id}"
                
                prov_data = await fetch_json_with_retry(client, prov_url, semaphore, logger, delay)
                
                db_local = SessionLocal()
                for item in prov_data:
                    code = item.get("kode_wilayah", "").strip()
                    name = item.get("nama", "").strip()
                    mst_code = str(item.get("mst_kode_wilayah", "")).strip() if item.get("mst_kode_wilayah") is not None else None
                    
                    db_local.merge(Province(kode_wilayah=code, nama=name, mst_kode_wilayah=mst_code))
                    if target_provinsi_ids and code not in target_provinsi_ids:
                        continue
                    provinces_to_process.append((code, name))
                    
                db_local.commit()
                db_local.close()
            
            await update_job_db(job_id, {
                "total_provinces": len(provinces_to_process),
                "current_step": "chaining"
            })
            
            logger.log(f"Found {len(provinces_to_process)} target provinces. Starting parallel chained requests...")
            
            # Start chain requests sequentially
            for code, name in provinces_to_process:
                if job_id in CANCELLED_JOBS:
                    break
                await chain_crawl_province(
                    client, code, name, bentuk_pendidikan_list, semester_id,
                    semaphore, delay, force_recrawl, job_id, logger
                )
            
            if job_id in CANCELLED_JOBS:
                raise asyncio.CancelledError()
                
            logger.log("Crawl completed successfully!")
            await update_job_db(job_id, {
                "status": "completed",
                "current_step": "idle"
            })
            
        except asyncio.CancelledError:
            logger.log("Crawl was cancelled by user.")
            await update_job_db(job_id, {
                "status": "cancelled",
                "current_step": "idle"
            })
        except Exception as e:
            logger.log(f"Crawl failed: {str(e)}")
            await update_job_db(job_id, {
                "status": "failed",
                "current_step": "idle",
                "error_message": str(e)
            })
        finally:
            if job_id in CANCELLED_JOBS:
                CANCELLED_JOBS.remove(job_id)


def start_background_crawl(
    job_id: str,
    target_provinsi_ids: list = None,
    bentuk_pendidikan_list: list = None,
    semester_id: str = "20252",
    concurrency_limit: int = 5,
    delay: float = 0.5,
    force_recrawl: bool = False
):
    loop = asyncio.get_event_loop()
    loop.create_task(
        crawl_dapodik_task(
            job_id,
            target_provinsi_ids,
            bentuk_pendidikan_list,
            semester_id,
            concurrency_limit,
            delay,
            force_recrawl
        )
    )


def cancel_job(job_id: str):
    CANCELLED_JOBS.add(job_id)

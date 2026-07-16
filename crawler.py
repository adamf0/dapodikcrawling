import asyncio
import os
import datetime
import re
from curl_cffi.requests import AsyncSession
from sqlalchemy.orm import Session
from database import SessionLocal
from models import Province, Kabupaten, Kecamatan, Sekolah, CrawlJob, CrawledKecamatan

# Global set for cancelled jobs
CANCELLED_JOBS = set()

# Global lock for updating CrawlJob table in SQLite to prevent concurrent lock errors
DB_LOCK = asyncio.Lock()

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

def safe_str(val) -> str:
    if val is None:
        return ""
    return str(val).strip()


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


async def get_api_token(session: AsyncSession, logger: JobLogger) -> str:
    fallback = "6d04941d7990b3b5270fe4a2d48dbe2a2eea962ebc7dce8d5b18d9a5017fec43d1c1be738f7479a6e14f8a64d81751fe906c95c416565db6747715be9aaf218846357cc5524d9bfae710652f165b248cc968a62f49d6ac738bf7ea3ac3c74cc9e4dfa7d14bc9bdea26b5f217971af447b4c1346a5611bce1a06a76a099e59b3f"
    try:
        r = await session.get("https://dapo.kemendikdasmen.go.id/env.js", timeout=15)
        if r.status_code == 200:
            m = re.search(r'VITE_API_TOKEN:\s*"(.*?)"', r.text)
            if m:
                token = m.group(1).strip()
                logger.log(f"Dynamic API Token fetched successfully: {token[:10]}...")
                return token
    except Exception as e:
        logger.log(f"Warning: Failed to fetch dynamic API token: {e}. Using fallback token.")
    return fallback


async def fetch_json_with_retry(
    session: AsyncSession,
    url: str,
    semaphore: asyncio.Semaphore,
    logger: JobLogger,
    delay: float = 0.5,
    max_retries: int = 10,
    backoff_factor: float = 2.0
) -> dict:
    async with semaphore:
        for attempt in range(1, max_retries + 1):
            try:
                response = await session.get(url, timeout=30.0)
                if response.status_code == 200:
                    if delay > 0:
                        await asyncio.sleep(delay)
                    return response.json()
                elif response.status_code == 503:
                    sleep_time = max(8.0, backoff_factor ** attempt * 4.0)
                    logger.log(f"[RATE-LIMIT] 503 on {url}. Backing off {sleep_time}s... (Attempt {attempt}/{max_retries})")
                    await asyncio.sleep(sleep_time)
                elif response.status_code in [401, 403]:
                    ct = response.headers.get("content-type", "")
                    if "application/json" in ct:
                        logger.log(f"[TOKEN EXPIRED] Received {response.status_code} on {url}. Attempting token refresh...")
                        new_token = await get_api_token(session, logger)
                        session.headers["Authorization"] = f"Bearer {new_token}"
                        continue
                    else:
                        sleep_time = max(10.0, backoff_factor ** attempt * 5.0)
                        logger.log(f"[WAF BLOCK] Safeline WAF 403 on {url}. Backing off {sleep_time}s... (Attempt {attempt}/{max_retries})")
                        await asyncio.sleep(sleep_time)
                else:
                    logger.log(f"HTTP Error {response.status_code} for URL: {url} (Attempt {attempt}/{max_retries})")
            except Exception as e:
                logger.log(f"Request Exception: {str(e)} for URL: {url} (Attempt {attempt}/{max_retries})")
            
            if attempt < max_retries:
                sleep_time = backoff_factor ** attempt
                await asyncio.sleep(sleep_time)
                
        raise Exception(f"Failed to fetch JSON from {url} after {max_retries} attempts.")


# ----------------------------------------------------
# MODULAR STAGE WORKERS
# ----------------------------------------------------

async def crawl_provinces_step(
    session: AsyncSession,
    target_provinsi_ids: list,
    semester_id: str,
    semaphore: asyncio.Semaphore,
    delay: float,
    force_recrawl: bool,
    job_id: str,
    logger: JobLogger
) -> list:
    """Stage 1: Crawl Provinces"""
    if job_id in CANCELLED_JOBS:
        return []

    await update_job_db(job_id, {"current_step": "provinces"})
    logger.log("=== STAGE 1: Crawling Provinces ===")

    db_local = SessionLocal()
    existing_provs = []
    try:
        if not force_recrawl:
            existing_provs = db_local.query(Province).all()
    finally:
        db_local.close()

    provinces_to_process = []
    if existing_provs:
        logger.log(f"Cache Hit (DB): Found {len(existing_provs)} Provinces in DB.")
        for prov in existing_provs:
            if target_provinsi_ids and prov.kode_wilayah not in target_provinsi_ids:
                continue
            provinces_to_process.append((prov.kode_wilayah, prov.nama))
    else:
        logger.log("Fetching Provinces from Dapodik API...")
        prov_url = "https://dapo.kemendikdasmen.go.id/api/progress-pengiriman/provinsi"
        prov_payload = await fetch_json_with_retry(session, prov_url, semaphore, logger, delay)
        prov_data = prov_payload.get("data", [])

        db_local = SessionLocal()
        try:
            for item in prov_data:
                code = safe_str(item.get("kode_provinsi"))
                name = safe_str(item.get("provinsi"))
                mst_code = "000000"

                db_local.merge(Province(kode_wilayah=code, nama=name, mst_kode_wilayah=mst_code))
                if target_provinsi_ids and code not in target_provinsi_ids:
                    continue
                provinces_to_process.append((code, name))
            db_local.commit()
        finally:
            db_local.close()

    total = len(provinces_to_process)
    await update_job_db(job_id, {
        "total_provinces": total,
        "processed_provinces": total
    })
    logger.log(f"Stage 1 Complete: {total} Provinces saved/verified in database.")
    return provinces_to_process


async def crawl_kabupatens_step(
    session: AsyncSession,
    target_provinsi_ids: list,
    semester_id: str,
    semaphore: asyncio.Semaphore,
    delay: float,
    force_recrawl: bool,
    job_id: str,
    logger: JobLogger
) -> list:
    """Stage 2: Crawl Kabupatens for available Provinces"""
    if job_id in CANCELLED_JOBS:
        return []

    await update_job_db(job_id, {"current_step": "kabupatens"})
    logger.log("=== STAGE 2: Crawling Kabupatens ===")

    db_local = SessionLocal()
    provinces = []
    try:
        query = db_local.query(Province)
        if target_provinsi_ids:
            query = query.filter(Province.kode_wilayah.in_(target_provinsi_ids))
        provinces = query.all()
    finally:
        db_local.close()

    if not provinces:
        logger.log("Warning: No Provinces found in DB to crawl Kabupatens. Run Stage 1 (Crawling Provinsi) first!")
        return []

    await update_job_db(job_id, {"total_provinces": len(provinces)})
    all_kabupatens = []

    for prov in provinces:
        if job_id in CANCELLED_JOBS:
            break

        db_local = SessionLocal()
        existing_kabs = []
        try:
            if not force_recrawl:
                existing_kabs = db_local.query(Kabupaten).filter(Kabupaten.mst_kode_wilayah == prov.kode_wilayah).all()
        finally:
            db_local.close()

        if existing_kabs:
            logger.log(f"Cache Hit (DB): Found {len(existing_kabs)} Kabupatens for Province {prov.nama} in DB.")
            for kab in existing_kabs:
                all_kabupatens.append((kab.kode_wilayah, kab.nama))
        else:
            logger.log(f"Crawl: Fetching Kabupatens for Province: {prov.nama} ({prov.kode_wilayah})...")
            kab_url = f"https://dapo.kemendikdasmen.go.id/api/progress-pengiriman/kabupaten?kode_provinsi={prov.kode_wilayah}"
            kab_payload = await fetch_json_with_retry(session, kab_url, semaphore, logger, delay)
            kab_data = kab_payload.get("data", [])

            db_local = SessionLocal()
            try:
                for item in kab_data:
                    k_code = safe_str(item.get("kode_kabupaten"))
                    k_name = safe_str(item.get("kabupaten"))
                    k_mst = safe_str(item.get("kode_provinsi"))
                    db_local.merge(Kabupaten(kode_wilayah=k_code, nama=k_name, mst_kode_wilayah=k_mst))
                    all_kabupatens.append((k_code, k_name))
                db_local.commit()
            finally:
                db_local.close()

        await update_job_db(job_id, {
            "increment_processed_provinces": 1,
            "total_kabupatens": len(all_kabupatens)
        })

    logger.log(f"Stage 2 Complete: {len(all_kabupatens)} Kabupatens saved/verified in database.")
    return all_kabupatens


async def crawl_kecamatans_step(
    session: AsyncSession,
    target_provinsi_ids: list,
    semester_id: str,
    semaphore: asyncio.Semaphore,
    delay: float,
    force_recrawl: bool,
    job_id: str,
    logger: JobLogger
) -> list:
    """Stage 3: Crawl Kecamatans for available Kabupatens"""
    if job_id in CANCELLED_JOBS:
        return []

    await update_job_db(job_id, {"current_step": "kecamatans"})
    logger.log("=== STAGE 3: Crawling Kecamatans ===")

    db_local = SessionLocal()
    kabupatens = []
    try:
        query = db_local.query(Kabupaten)
        if target_provinsi_ids:
            query = query.filter(Kabupaten.mst_kode_wilayah.in_(target_provinsi_ids))
        kabupatens = query.all()
    finally:
        db_local.close()

    if not kabupatens:
        logger.log("Warning: No Kabupatens found in DB to crawl Kecamatans. Run Stage 2 (Crawling Kabupaten) first!")
        return []

    await update_job_db(job_id, {"total_kabupatens": len(kabupatens)})
    all_kecamatans = []

    for kab in kabupatens:
        if job_id in CANCELLED_JOBS:
            break

        db_local = SessionLocal()
        existing_kecs = []
        try:
            if not force_recrawl:
                existing_kecs = db_local.query(Kecamatan).filter(Kecamatan.mst_kode_wilayah == kab.kode_wilayah).all()
        finally:
            db_local.close()

        if existing_kecs:
            logger.log(f"Cache Hit (DB): Found {len(existing_kecs)} Kecamatans for Kabupaten {kab.nama} in DB.")
            for kec in existing_kecs:
                all_kecamatans.append((kec.kode_wilayah, kec.nama))
        else:
            logger.log(f"Crawl: Fetching Kecamatans for Kabupaten: {kab.nama} ({kab.kode_wilayah})...")
            kec_url = f"https://dapo.kemendikdasmen.go.id/api/progress-pengiriman/kecamatan?kode_kabupaten={kab.kode_wilayah}"
            kec_payload = await fetch_json_with_retry(session, kec_url, semaphore, logger, delay)
            kec_data = kec_payload.get("data", [])

            db_local = SessionLocal()
            try:
                for item in kec_data:
                    kc_code = safe_str(item.get("kode_kecamatan"))
                    kc_name = safe_str(item.get("kecamatan"))
                    kc_mst = safe_str(item.get("kode_kabupaten"))
                    db_local.merge(Kecamatan(kode_wilayah=kc_code, nama=kc_name, mst_kode_wilayah=kc_mst))
                    all_kecamatans.append((kc_code, kc_name))
                db_local.commit()
            finally:
                db_local.close()

        await update_job_db(job_id, {
            "increment_processed_kabupatens": 1,
            "total_kecamatans": len(all_kecamatans)
        })

    logger.log(f"Stage 3 Complete: {len(all_kecamatans)} Kecamatans saved/verified in database.")
    return all_kecamatans


async def fetch_school_details(
    session: AsyncSession,
    npsn: str,
    semester_id: str,
    semaphore: asyncio.Semaphore,
    delay: float,
    logger: JobLogger
) -> dict:
    details = {
        "sekolah_id": None,
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
        "waktu": None,
        "jml_lab": 0,
        "sinkron_terakhir": None
    }
    if not npsn:
        return details

    url = f"https://dapo.kemendikdasmen.go.id/api/detail-sekolah?npsn={npsn}"
    try:
        payload = await fetch_json_with_retry(session, url, semaphore, logger, delay)
        records = payload.get("data", [])
        if not records:
            return details

        # Find the record matching semester_id, otherwise default to records[0] (latest)
        selected_record = records[0]
        for r in records:
            if str(r.get("semester")) == str(semester_id):
                selected_record = r
                break

        details["sekolah_id"] = selected_record.get("sekolah_id")
        details["status_kepemilikan"] = selected_record.get("status_kepemilikan")
        details["sk_pendirian_sekolah"] = selected_record.get("sk_pendirian_sekolah")
        details["tanggal_sk_pendirian"] = selected_record.get("tlg_sk_pendirian_sekolah")
        details["sk_izin_operasional"] = selected_record.get("sk_izin_operasional")
        details["tanggal_sk_izin_operasional"] = selected_record.get("tlg_sk_izin_operasional")
        details["desa_kelurahan"] = selected_record.get("desa_kelurahan")
        details["kecamatan_scraped"] = selected_record.get("kecamatan")
        details["kabupaten_scraped"] = selected_record.get("kabupaten")
        details["provinsi_scraped"] = selected_record.get("provinsi")
        details["kode_pos"] = selected_record.get("kode_pos")
        details["kepsek"] = selected_record.get("nama_kepsek")
        details["akreditasi"] = selected_record.get("akreditasi")
        details["sinkron_terakhir"] = selected_record.get("tanggal_update")

        # Lab calculations
        lab_fields = ["lab_ipa", "lab_biologi", "lab_fisika", "lab_kimia", "lab_multi", "lab_kom", "lab_bahasa"]
        details["jml_lab"] = sum(int(selected_record.get(f, 0) or 0) for f in lab_fields if selected_record.get(f) is not None)
    except Exception as e:
        logger.log(f"Error fetching profile details for NPSN {npsn}: {e}")
    return details


async def chain_crawl_kecamatan(
    session: AsyncSession,
    kc_code: str,
    kc_name: str,
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

    # Check cache unless force_recrawl
    uncrawled_bentuk_list = []
    if not force_recrawl:
        db = SessionLocal()
        try:
            for bentuk in bentuk_pendidikan_list:
                crawled = db.query(CrawledKecamatan).filter(
                    CrawledKecamatan.kecamatan_id == kc_code,
                    CrawledKecamatan.bentuk_pendidikan == bentuk
                ).first()
                if not crawled:
                    uncrawled_bentuk_list.append(bentuk)
        finally:
            db.close()
    else:
        uncrawled_bentuk_list = bentuk_pendidikan_list.copy()

    if not uncrawled_bentuk_list:
        logger.log(f"Cache Hit: Skipping schools for Kecamatan: {kc_name}")
        await update_job_db(job_id, {"increment_processed_sekolahs": len(bentuk_pendidikan_list)})
        return

    # Uppercase target jenjang
    jenjang_param = ",".join([b.upper() for b in uncrawled_bentuk_list])
    logger.log(f"Crawl: Fetching schools for Kecamatan: {kc_name} ({kc_code}) for shapes: {jenjang_param}...")
    
    school_url = f"https://dapo.kemendikdasmen.go.id/api/progress-pengiriman/kecamatan/school?kode_kecamatan={kc_code}&jenjang={jenjang_param}"
    
    try:
        school_payload = await fetch_json_with_retry(session, school_url, semaphore, logger, delay)
        school_data = school_payload.get("data", [])
        if not school_data:
            school_data = []

        db_local = SessionLocal()
        cache_hits = {}
        try:
            npsns = [safe_str(item.get("npsn")) for item in school_data if item.get("npsn")]
            if npsns and not force_recrawl:
                existing_schools = db_local.query(Sekolah).filter(Sekolah.npsn.in_(npsns)).all()
                for s in existing_schools:
                    if s.status_kepemilikan is not None:
                        cache_hits[s.npsn] = s
        finally:
            db_local.close()

        resolved_schools = []
        async def process_single_school(item):
            s_npsn = safe_str(item.get("npsn"))
            s_name = safe_str(item.get("nama"))
            s_bentuk = safe_str(item.get("bentuk_pendidikan"))
            s_status = safe_str(item.get("status_sekolah"))
            
            pd = int(item.get("pd") or 0)
            rombel = int(item.get("rombel") or 0)
            rk = int(item.get("ruang_kelas") or 0)
            perpus = int(item.get("perpus") or 0)
            
            # Check cache map
            details = None
            if s_npsn in cache_hits:
                existing = cache_hits[s_npsn]
                details = {
                    "sekolah_id": existing.sekolah_id,
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
                    "akreditasi": existing.akreditasi,
                    "sinkron_terakhir": existing.sinkron_terakhir,
                    "jml_lab": existing.jml_lab
                }
                
            if not details or not details.get("sekolah_id"):
                logger.log(f"Fetching profile via API: {s_name} ({s_npsn})...")
                details = await fetch_school_details(session, s_npsn, semester_id, semaphore, delay, logger)
                if details.get("sekolah_id"):
                    logger.log(f"Fetched successfully: {s_name} ({s_npsn})")
                else:
                    logger.log(f"Fetched profile failed (using default empty profile): {s_name} ({s_npsn})")
            else:
                logger.log(f"Cache Hit (DB): Skipping profile API fetch for {s_name} ({s_npsn})")
                
            sekolah_id = details.get("sekolah_id") or f"npsn-{s_npsn}"
            
            jum_guru = int(item.get("jum_guru") or 0)
            jum_tendik = int(item.get("jum_tendik") or 0)
            ptk = jum_guru + jum_tendik
            
            resolved_schools.append(Sekolah(
                sekolah_id=sekolah_id,
                sekolah_id_enkrip=None,
                nama=s_name,
                npsn=s_npsn,
                bentuk_pendidikan=s_bentuk,
                status_sekolah=s_status,
                ptk=ptk,
                pegawai=jum_tendik,
                pd=pd,
                rombel=rombel,
                jml_rk=rk,
                jml_lab=details.get("jml_lab", 0),
                jml_perpus=perpus,
                sinkron_terakhir=details.get("sinkron_terakhir") or safe_str(item.get("tanggal_update")),
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
                operator=None,
                akreditasi=details["akreditasi"],
                kurikulum=None,
                waktu=None,
                kecamatan_id=kc_code
            ))
            
        await asyncio.gather(*(process_single_school(item) for item in school_data))
        
        db_local = SessionLocal()
        try:
            for s in resolved_schools:
                db_local.merge(s)
            for bentuk in uncrawled_bentuk_list:
                db_local.merge(CrawledKecamatan(kecamatan_id=kc_code, bentuk_pendidikan=bentuk))
            db_local.commit()
        finally:
            db_local.close()
    except Exception as e:
        logger.log(f"Error fetching schools for {kc_name}: {e}")
        
    await update_job_db(job_id, {"increment_processed_sekolahs": len(bentuk_pendidikan_list)})


async def crawl_sekolahs_step(
    session: AsyncSession,
    target_provinsi_ids: list,
    bentuk_pendidikan_list: list,
    semester_id: str,
    semaphore: asyncio.Semaphore,
    delay: float,
    force_recrawl: bool,
    job_id: str,
    logger: JobLogger
):
    """Stage 4: Crawl Sekolahs for available Kecamatans"""
    if job_id in CANCELLED_JOBS:
        return

    await update_job_db(job_id, {"current_step": "sekolahs"})
    logger.log("=== STAGE 4: Crawling Sekolahs ===")

    db_local = SessionLocal()
    kecamatans = []
    try:
        if target_provinsi_ids:
            kecamatans = db_local.query(Kecamatan).join(Kabupaten).filter(Kabupaten.mst_kode_wilayah.in_(target_provinsi_ids)).all()
        else:
            kecamatans = db_local.query(Kecamatan).all()
    finally:
        db_local.close()

    if not kecamatans:
        logger.log("Warning: No Kecamatans found in DB to crawl Sekolahs. Run Stage 3 (Crawling Kecamatan) first!")
        return

    total_sekolahs_target = len(kecamatans) * len(bentuk_pendidikan_list)
    await update_job_db(job_id, {
        "total_kecamatans": len(kecamatans),
        "total_sekolahs": total_sekolahs_target
    })

    for kec in kecamatans:
        if job_id in CANCELLED_JOBS:
            break
        await chain_crawl_kecamatan(
            session, kec.kode_wilayah, kec.nama, bentuk_pendidikan_list, semester_id,
            semaphore, delay, force_recrawl, job_id, logger
        )
        await update_job_db(job_id, {"increment_processed_kecamatans": 1})

    logger.log("Stage 4 Complete: Sekolahs scraped & saved in database.")


async def crawl_dapodik_task(
    job_id: str,
    step: str = "all",
    target_provinsi_ids: list = None,
    bentuk_pendidikan_list: list = None,
    semester_id: str = "20252",
    concurrency_limit: int = 5,
    delay: float = 0.5,
    force_recrawl: bool = False
):
    logger = JobLogger(job_id)
    logger.log(f"Starting Dapodik crawler job (Step: {step})...")
    
    if not bentuk_pendidikan_list:
        bentuk_pendidikan_list = ["sd", "smp", "sma"]
        
    logger.log(f"Stage Target: {step}")
    logger.log(f"Forms target: {bentuk_pendidikan_list}")
    logger.log(f"Semester ID: {semester_id}")
    logger.log(f"Concurrency limit: {concurrency_limit} requests")
    logger.log(f"Polite request delay: {delay} seconds")
    
    db: Session = SessionLocal()
    try:
        job = db.query(CrawlJob).filter(CrawlJob.id == job_id).first()
        if not job:
            logger.log("Job record not found. Exiting.")
            return

        # Initialize job counters
        job.status = "running"
        job.step = step
        job.current_step = step
        job.total_provinces = 0
        job.processed_provinces = 0
        job.total_kabupatens = 0
        job.processed_kabupatens = 0
        job.total_kecamatans = 0
        job.processed_kecamatans = 0
        job.total_sekolahs = 0
        job.processed_sekolahs = 0
        db.commit()
    finally:
        db.close()
    
    semaphore = asyncio.Semaphore(concurrency_limit)
    
    async with AsyncSession(impersonate="chrome") as session:
        try:
            if job_id in CANCELLED_JOBS:
                raise asyncio.CancelledError()

            # Dynamic API Token retrieval
            logger.log("Fetching API configuration and token...")
            token = await get_api_token(session, logger)
            session.headers.update({
                "Authorization": f"Bearer {token}",
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://dapo.kemendikdasmen.go.id/"
            })

            if step == "provinces":
                await crawl_provinces_step(session, target_provinsi_ids, semester_id, semaphore, delay, force_recrawl, job_id, logger)
            elif step == "kabupatens":
                await crawl_kabupatens_step(session, target_provinsi_ids, semester_id, semaphore, delay, force_recrawl, job_id, logger)
            elif step == "kecamatans":
                await crawl_kecamatans_step(session, target_provinsi_ids, semester_id, semaphore, delay, force_recrawl, job_id, logger)
            elif step == "sekolahs":
                await crawl_sekolahs_step(session, target_provinsi_ids, bentuk_pendidikan_list, semester_id, semaphore, delay, force_recrawl, job_id, logger)
            else: # "all"
                await crawl_provinces_step(session, target_provinsi_ids, semester_id, semaphore, delay, force_recrawl, job_id, logger)
                if job_id not in CANCELLED_JOBS:
                    await crawl_kabupatens_step(session, target_provinsi_ids, semester_id, semaphore, delay, force_recrawl, job_id, logger)
                if job_id not in CANCELLED_JOBS:
                    await crawl_kecamatans_step(session, target_provinsi_ids, semester_id, semaphore, delay, force_recrawl, job_id, logger)
                if job_id not in CANCELLED_JOBS:
                    await crawl_sekolahs_step(session, target_provinsi_ids, bentuk_pendidikan_list, semester_id, semaphore, delay, force_recrawl, job_id, logger)
            
            if job_id in CANCELLED_JOBS:
                raise asyncio.CancelledError()
                
            logger.log("Crawl job completed successfully!")
            await update_job_db(job_id, {
                "status": "completed",
                "current_step": "idle"
            })
            
        except asyncio.CancelledError:
            logger.log("Crawl job was cancelled by user.")
            await update_job_db(job_id, {
                "status": "cancelled",
                "current_step": "idle"
            })
        except Exception as e:
            logger.log(f"Crawl job failed: {str(e)}")
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
    step: str = "all",
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
            step,
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

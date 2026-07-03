import os
import uuid
import asyncio
import csv
import io
import json
from typing import Optional, List
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import func

from database import engine, get_db, SessionLocal
import models
from models import Base, Province, Kabupaten, Kecamatan, Sekolah, CrawlJob
from crawler import start_background_crawl, cancel_job

# Initialize DB tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Dapodik School Data Crawler & Browser",
    description="FastAPI application to crawl, save, search and export national school records from Dapodik.",
    version="2.0.0"
)
@app.on_event("startup")
async def startup_event():
    # Automatically resume any running/pending jobs in database on startup
    db = SessionLocal()
    try:
        active_jobs = db.query(CrawlJob).filter(CrawlJob.status.in_(["running", "pending"])).all()
        for job in active_jobs:
            print(f"Resuming active job: {job.id} in background...")
            prov_ids = json.loads(job.target_provinsi_ids) if job.target_provinsi_ids else None
            bentuk_list = json.loads(job.bentuk_pendidikan_list) if job.bentuk_pendidikan_list else None
            
            start_background_crawl(
                job_id=job.id,
                target_provinsi_ids=prov_ids,
                bentuk_pendidikan_list=bentuk_list,
                semester_id=job.semester_id,
                concurrency_limit=job.concurrency_limit,
                delay=job.delay,
                force_recrawl=job.force_recrawl
            )
    except Exception as e:
        print(f"Error during startup resume: {e}")
    finally:
        db.close()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request Models
class CrawlRequest(BaseModel):
    target_provinsi_ids: Optional[List[str]] = None
    bentuk_pendidikan_list: Optional[List[str]] = None
    semester_id: Optional[str] = "20252"
    concurrency_limit: Optional[int] = 5
    delay: Optional[float] = 0.5
    force_recrawl: Optional[bool] = False

# Ensure static folder exists
os.makedirs("static", exist_ok=True)

# ----------------------------------------------------
# CRAWLER CONTROL API
# ----------------------------------------------------

@app.post("/api/crawl/start")
async def start_crawl(req: CrawlRequest, db: Session = Depends(get_db)):
    # Check if there is already a running job
    running_job = db.query(CrawlJob).filter(CrawlJob.status == "running").first()
    if running_job:
        raise HTTPException(
            status_code=400,
            detail=f"Another job ({running_job.id}) is already running. Please cancel it or wait until it finishes."
        )

    job_id = str(uuid.uuid4())
    
    # Create CrawlJob record
    new_job = CrawlJob(
        id=job_id,
        status="pending",
        current_step="idle",
        target_provinsi_ids=json.dumps(req.target_provinsi_ids) if req.target_provinsi_ids else None,
        bentuk_pendidikan_list=json.dumps(req.bentuk_pendidikan_list) if req.bentuk_pendidikan_list else None,
        semester_id=req.semester_id,
        concurrency_limit=req.concurrency_limit,
        delay=req.delay,
        force_recrawl=req.force_recrawl
    )
    db.add(new_job)
    db.commit()

    # Trigger background crawler
    start_background_crawl(
        job_id=job_id,
        target_provinsi_ids=req.target_provinsi_ids,
        bentuk_pendidikan_list=req.bentuk_pendidikan_list,
        semester_id=req.semester_id,
        concurrency_limit=req.concurrency_limit,
        delay=req.delay,
        force_recrawl=req.force_recrawl
    )

    return {"job_id": job_id, "message": "Crawl job started in background."}


@app.post("/api/crawl/cancel/{job_id}")
async def cancel_crawl(job_id: str, db: Session = Depends(get_db)):
    job = db.query(CrawlJob).filter(CrawlJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    if job.status not in ["pending", "running"]:
        raise HTTPException(status_code=400, detail=f"Cannot cancel job in '{job.status}' status.")

    cancel_job(job_id)
    
    job.status = "cancelled"
    job.current_step = "idle"
    db.commit()
    
    return {"message": f"Cancellation request sent for job {job_id}"}


@app.get("/api/crawl/jobs")
async def get_jobs(db: Session = Depends(get_db)):
    jobs = db.query(CrawlJob).order_by(models.CrawlJob.created_at.desc()).limit(10).all()
    return jobs


@app.get("/api/crawl/status/{job_id}")
async def get_job_status(job_id: str, db: Session = Depends(get_db)):
    job = db.query(CrawlJob).filter(CrawlJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return {
        "id": job.id,
        "status": job.status,
        "current_step": job.current_step,
        "provinces": {"total": job.total_provinces, "processed": job.processed_provinces},
        "kabupatens": {"total": job.total_kabupatens, "processed": job.processed_kabupatens},
        "kecamatans": {"total": job.total_kecamatans, "processed": job.processed_kecamatans},
        "sekolahs": {"total": job.total_sekolahs, "processed": job.processed_sekolahs},
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "error_message": job.error_message
    }


@app.get("/api/crawl/stream/{job_id}")
async def stream_job_logs(job_id: str):
    async def log_generator():
        log_file = f"logs/{job_id}.log"
        for _ in range(10):
            if os.path.exists(log_file):
                break
            await asyncio.sleep(0.5)
            
        if not os.path.exists(log_file):
            yield "data: Waiting for log file initialization...\n\n"
            return
            
        last_pos = 0
        while True:
            if not os.path.exists(log_file):
                await asyncio.sleep(0.5)
                continue
                
            with open(log_file, "r", encoding="utf-8") as f:
                f.seek(last_pos)
                lines = f.readlines()
                last_pos = f.tell()
                
            for line in lines:
                yield f"data: {line.strip()}\n\n"
                
            db = SessionLocal()
            job = db.query(CrawlJob).filter(CrawlJob.id == job_id).first()
            status = job.status if job else "failed"
            db.close()
            
            if status in ["completed", "failed", "cancelled"] and not lines:
                # Read any last remaining lines before exiting
                with open(log_file, "r", encoding="utf-8") as f:
                    f.seek(last_pos)
                    lines = f.readlines()
                for line in lines:
                    yield f"data: {line.strip()}\n\n"
                yield f"data: [SYSTEM] Job finished with status: {status}\n\n"
                break
                
            await asyncio.sleep(0.5)

    return StreamingResponse(log_generator(), media_type="text/event-stream")


# ----------------------------------------------------
# DATABASE EXPLORER API
# ----------------------------------------------------

@app.get("/api/stats")
async def get_stats(db: Session = Depends(get_db)):
    prov_count = db.query(func.count(Province.kode_wilayah)).scalar()
    kab_count = db.query(func.count(Kabupaten.kode_wilayah)).scalar()
    kec_count = db.query(func.count(Kecamatan.kode_wilayah)).scalar()
    school_count = db.query(func.count(Sekolah.sekolah_id)).scalar()
    
    shapes_data = db.query(Sekolah.bentuk_pendidikan, func.count(Sekolah.sekolah_id)).group_by(Sekolah.bentuk_pendidikan).all()
    shapes = {shape: count for shape, count in shapes_data}
    
    status_data = db.query(Sekolah.status_sekolah, func.count(Sekolah.sekolah_id)).group_by(Sekolah.status_sekolah).all()
    status = {stat: count for stat, count in status_data}

    return {
        "totals": {
            "provinces": prov_count,
            "kabupatens": kab_count,
            "kecamatans": kec_count,
            "sekolahs": school_count
        },
        "shapes": shapes,
        "status": status
    }


@app.get("/api/provinces")
async def get_provinces(search: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(Province)
    if search:
        query = query.filter(Province.nama.ilike(f"%{search}%"))
    return query.order_by(Province.nama).all()


@app.get("/api/kabupatens")
async def get_kabupatens(provinsi_id: Optional[str] = None, search: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(Kabupaten)
    if provinsi_id:
        query = query.filter(Kabupaten.mst_kode_wilayah == provinsi_id)
    if search:
        query = query.filter(Kabupaten.nama.ilike(f"%{search}%"))
    return query.order_by(Kabupaten.nama).all()


@app.get("/api/kecamatans")
async def get_kecamatans(kabupaten_id: Optional[str] = None, search: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(Kecamatan)
    if kabupaten_id:
        query = query.filter(Kecamatan.mst_kode_wilayah == kabupaten_id)
    if search:
        query = query.filter(Kecamatan.nama.ilike(f"%{search}%"))
    return query.order_by(Kecamatan.nama).all()


@app.get("/api/sekolahs")
async def get_sekolahs(
    kecamatan_id: Optional[str] = None,
    bentuk_pendidikan: Optional[str] = None,
    status_sekolah: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    query = db.query(Sekolah)
    
    if kecamatan_id:
        query = query.filter(Sekolah.kecamatan_id == kecamatan_id)
    if bentuk_pendidikan:
        query = query.filter(Sekolah.bentuk_pendidikan == bentuk_pendidikan.upper())
    if status_sekolah:
        query = query.filter(Sekolah.status_sekolah == status_sekolah.capitalize())
    if search:
        query = query.filter(
            (Sekolah.nama.ilike(f"%{search}%")) | 
            (Sekolah.npsn.ilike(f"%{search}%"))
        )
        
    total = query.count()
    items = query.order_by(Sekolah.nama).offset(offset).limit(limit).all()
    
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": items
    }


@app.get("/api/export")
async def export_csv(db: Session = Depends(get_db)):
    """Exports SQLite schools database to a CSV file."""
    def csv_generator():
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers
        writer.writerow([
            "Sekolah ID", "NPSN", "Nama Sekolah", "Bentuk Pendidikan", "Status",
            "Kecamatan", "Kabupaten", "Provinsi", "Desa/Kelurahan", "Kode Pos",
            "Kepala Sekolah", "Operator", "Akreditasi", "Kurikulum", "Waktu Penyelenggaraan",
            "PTK", "Pegawai", "Peserta Didik (PD)", "Rombel", "Jml Ruang Kelas", "Jml Lab", "Jml Perpus",
            "SK Pendirian", "Tgl SK Pendirian", "SK Izin Operasional", "Tgl SK Izin Operasional",
            "Terakhir Sinkron"
        ])
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)
        
        # Streaming records
        chunk_size = 500
        offset = 0
        while True:
            schools = db.query(Sekolah).offset(offset).limit(chunk_size).all()
            if not schools:
                break
                
            for s in schools:
                writer.writerow([
                    s.sekolah_id, s.npsn, s.nama, s.bentuk_pendidikan, s.status_sekolah,
                    s.kecamatan_scraped or s.kecamatan_id, s.kabupaten_scraped, s.provinsi_scraped,
                    s.desa_kelurahan, s.kode_pos, s.kepsek, s.operator, s.akreditasi, s.kurikulum, s.waktu,
                    s.ptk, s.pegawai, s.pd, s.rombel, s.jml_rk, s.jml_lab, s.jml_perpus,
                    s.sk_pendirian_sekolah, s.tanggal_sk_pendirian, s.sk_izin_operasional, s.tanggal_sk_izin_operasional,
                    s.sinkron_terakhir
                ])
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
            offset += chunk_size

    headers = {
        'Content-Disposition': 'attachment; filename="sekolah_dapodik.csv"'
    }
    return StreamingResponse(csv_generator(), media_type="text/csv", headers=headers)


# Serve static web dashboard
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_root():
    return FileResponse("static/index.html")

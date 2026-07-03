from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from database import Base
import datetime

class Province(Base):
    __tablename__ = "provinces"
    
    kode_wilayah = Column(String(50), primary_key=True, index=True)
    nama = Column(String(255), nullable=False)
    mst_kode_wilayah = Column(String(50), nullable=True)
    
    kabupatens = relationship("Kabupaten", back_populates="province", cascade="all, delete-orphan")


class Kabupaten(Base):
    __tablename__ = "kabupatens"
    
    kode_wilayah = Column(String(50), primary_key=True, index=True)
    nama = Column(String(255), nullable=False)
    mst_kode_wilayah = Column(String(50), ForeignKey("provinces.kode_wilayah", ondelete="CASCADE"), nullable=False)
    
    province = relationship("Province", back_populates="kabupatens")
    kecamatans = relationship("Kecamatan", back_populates="kabupaten", cascade="all, delete-orphan")


class Kecamatan(Base):
    __tablename__ = "kecamatans"
    
    kode_wilayah = Column(String(50), primary_key=True, index=True)
    nama = Column(String(255), nullable=False)
    mst_kode_wilayah = Column(String(50), ForeignKey("kabupatens.kode_wilayah", ondelete="CASCADE"), nullable=False)
    
    kabupaten = relationship("Kabupaten", back_populates="kecamatans")
    sekolahs = relationship("Sekolah", back_populates="kecamatan", cascade="all, delete-orphan")


class Sekolah(Base):
    __tablename__ = "sekolahs"
    
    sekolah_id = Column(String(100), primary_key=True, index=True)
    sekolah_id_enkrip = Column(String(255), nullable=True)
    nama = Column(String(255), nullable=False)
    npsn = Column(String(50), nullable=True, index=True)
    bentuk_pendidikan = Column(String(50), nullable=False, index=True)
    status_sekolah = Column(String(50), nullable=True)
    
    ptk = Column(Integer, default=0)
    pegawai = Column(Integer, default=0)
    pd = Column(Integer, default=0)
    rombel = Column(Integer, default=0)
    jml_rk = Column(Integer, default=0)
    jml_lab = Column(Integer, default=0)
    jml_perpus = Column(Integer, default=0)
    sinkron_terakhir = Column(String(100), nullable=True)
    
    # Scraped Fields
    status_kepemilikan = Column(String(255), nullable=True)
    sk_pendirian_sekolah = Column(String(255), nullable=True)
    tanggal_sk_pendirian = Column(String(100), nullable=True)
    sk_izin_operasional = Column(String(255), nullable=True)
    tanggal_sk_izin_operasional = Column(String(100), nullable=True)
    desa_kelurahan = Column(String(255), nullable=True)
    kecamatan_scraped = Column(String(255), nullable=True)
    kabupaten_scraped = Column(String(255), nullable=True)
    provinsi_scraped = Column(String(255), nullable=True)
    kode_pos = Column(String(50), nullable=True)
    
    kepsek = Column(String(255), nullable=True)
    operator = Column(String(255), nullable=True)
    akreditasi = Column(String(50), nullable=True)
    kurikulum = Column(String(255), nullable=True)
    waktu = Column(String(100), nullable=True)
    
    kecamatan_id = Column(String(50), ForeignKey("kecamatans.kode_wilayah", ondelete="CASCADE"), nullable=False)
    kecamatan = relationship("Kecamatan", back_populates="sekolahs")


class CrawledKecamatan(Base):
    __tablename__ = "crawled_kecamatans"
    
    kecamatan_id = Column(String(50), ForeignKey("kecamatans.kode_wilayah", ondelete="CASCADE"), primary_key=True)
    bentuk_pendidikan = Column(String(50), primary_key=True)
    crawled_at = Column(DateTime, default=datetime.datetime.utcnow)


class CrawlJob(Base):
    __tablename__ = "crawl_jobs"
    
    id = Column(String(100), primary_key=True, index=True)
    status = Column(String(50), default="pending")  # pending, running, completed, failed, cancelled
    current_step = Column(String(100), default="idle")
    
    total_provinces = Column(Integer, default=0)
    processed_provinces = Column(Integer, default=0)
    total_kabupatens = Column(Integer, default=0)
    processed_kabupatens = Column(Integer, default=0)
    total_kecamatans = Column(Integer, default=0)
    processed_kecamatans = Column(Integer, default=0)
    total_sekolahs = Column(Integer, default=0)
    processed_sekolahs = Column(Integer, default=0)
    
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    error_message = Column(Text, nullable=True)

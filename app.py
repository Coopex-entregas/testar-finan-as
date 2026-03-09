from __future__ import annotations

import os
import io
import re
import csv
import json
import time
import difflib
import mimetypes
import unicodedata
from pathlib import Path
from functools import wraps
from collections import defaultdict, namedtuple
from datetime import datetime, date, timedelta, time as dtime
from types import SimpleNamespace
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from dateutil.relativedelta import relativedelta

from flask import (
    Flask, render_template, request, redirect, url_for, session,
    flash, send_file, abort, jsonify, current_app
)
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

from sqlalchemy import (
    text, func, or_, and_, case, literal, inspect as sa_inspect, event
)
from sqlalchemy import delete as sa_delete
from sqlalchemy.engine import Engine
from sqlalchemy.exc import (
    OperationalError, SQLAlchemyError, IntegrityError,
    ProgrammingError, DisconnectionError
)
from sqlalchemy.pool import QueuePool

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment, PatternFill

mimetypes.add_type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx")
mimetypes.add_type("application/vnd.ms-excel", ".xls")
mimetypes.add_type("application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx")
mimetypes.add_type("application/msword", ".doc")
mimetypes.add_type("application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx")
mimetypes.add_type("application/vnd.ms-powerpoint", ".ppt")


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

UPLOAD_DIR = os.path.join(BASE_DIR, "static", "uploads")
DOCS_DIR = os.path.join(UPLOAD_DIR, "docs")
STATIC_TABLES = os.path.join(BASE_DIR, "static", "uploads", "tabelas")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(DOCS_DIR, exist_ok=True)
os.makedirs(STATIC_TABLES, exist_ok=True)

PERSIST_ROOT = os.environ.get("PERSIST_ROOT", "/var/data")
if not os.path.isdir(PERSIST_ROOT):
    PERSIST_ROOT = os.path.join(BASE_DIR, "data")
os.makedirs(PERSIST_ROOT, exist_ok=True)

DOCS_PERSIST_DIR = os.path.join(PERSIST_ROOT, "docs")
TABELAS_DIR = os.path.join(PERSIST_ROOT, "tabelas")
os.makedirs(DOCS_PERSIST_DIR, exist_ok=True)
os.makedirs(TABELAS_DIR, exist_ok=True)


def _merge_qs(url: str, extra: dict[str, str]) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    for k, v in (extra or {}).items():
        q.setdefault(k, v)
    return urlunparse(p._replace(query=urlencode(q, doseq=True)))


def _build_db_uri() -> str:
    raw = os.environ.get("DATABASE_URL")
    if not raw:
        return "sqlite:///" + os.path.join(BASE_DIR, "app.db")

    if raw.startswith("postgres://"):
        raw = raw.replace("postgres://", "postgresql+psycopg://", 1)
    elif raw.startswith("postgresql://") and "+psycopg" not in raw:
        raw = raw.replace("postgresql://", "postgresql+psycopg://", 1)

    extras = {
        "sslmode": "require",
        "keepalives": "1",
        "keepalives_idle": "30",
        "keepalives_interval": "10",
        "keepalives_count": "3",
        "application_name": os.environ.get("APP_NAME", "coopex"),
    }
    return _merge_qs(raw, extras)


app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = os.environ.get("SECRET_KEY", "coopex-secret")

URI = _build_db_uri()
if "sqlite" in URI and os.environ.get("FLASK_ENV") == "production":
    raise RuntimeError("DATABASE_URL ausente em produção")

workers = int(os.environ.get("WEB_CONCURRENCY", "1") or "1")
threads = int(os.environ.get("GTHREADS", "1") or "1")
req_concurrency = max(1, workers * threads)

target_total = int(os.environ.get("DB_TARGET_CONNS", "40") or "40")
per_worker_target = max(5, min(target_total // max(1, workers), 15))

default_pool_size = min(per_worker_target, req_concurrency + 2)
default_max_overflow = max(5, default_pool_size)

pool_size = int(os.environ.get("DB_POOL_SIZE", str(default_pool_size)))
max_overflow = int(os.environ.get("DB_MAX_OVERFLOW", str(default_max_overflow)))
pool_recycle = int(os.environ.get("SQL_POOL_RECYCLE", "240"))
pool_timeout = int(os.environ.get("SQL_POOL_TIMEOUT", "20"))

app.config.update(
    SQLALCHEMY_DATABASE_URI=URI,
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    JSON_SORT_KEYS=False,
    MAX_CONTENT_LENGTH=32 * 1024 * 1024,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("FLASK_SECURE_COOKIES", "1") == "1",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
    SQLALCHEMY_ENGINE_OPTIONS={
        "poolclass": QueuePool,
        "pool_size": pool_size,
        "max_overflow": max_overflow,
        "pool_timeout": pool_timeout,
        "pool_pre_ping": True,
        "pool_use_lifo": True,
        "pool_recycle": pool_recycle,
        "connect_args": {
            "connect_timeout": int(os.getenv("PGCONNECT_TIMEOUT", "5")),
            "options": "-c statement_timeout=15000",
        },
    },
)

db = SQLAlchemy(app)


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.get("/readyz")
def readyz():
    try:
        db.session.execute(text("SELECT 1"))
        return "ready", 200
    except Exception:
        return "not-ready", 503


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_con, con_record):
    try:
        if "sqlite" in app.config["SQLALCHEMY_DATABASE_URI"]:
            cur = dbapi_con.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()
    except Exception:
        pass


def _is_sqlite() -> bool:
    try:
        return db.session.get_bind().dialect.name == "sqlite"
    except Exception:
        return "sqlite" in (app.config.get("SQLALCHEMY_DATABASE_URI") or "")


INSS_ALIQ = float(os.environ.get("ALIQUOTA_INSS", "0.04"))
SEST_ALIQ = float(os.environ.get("ALIQUOTA_SEST", "0.005"))


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def _fmt_time(t) -> str:
    if not t:
        return ""
    if hasattr(t, "strftime"):
        try:
            return t.strftime("%H:%M")
        except Exception:
            pass
    return str(t)


def _dow(d: date) -> str:
    return str(d.weekday()) if d else ""


def role_required(role: str):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if session.get("user_tipo") != role:
                return redirect(url_for("login"))
            return fn(*args, **kwargs)
        return wrapper
    return deco


def admin_required(fn):
    return role_required("admin")(fn)


def _normalize_name(s: str) -> list[str]:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)
    return [p.lower() for p in s.split() if p.strip()]


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", str(s or "").strip().lower())
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


def _norm_login(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower().strip()
    s = re.sub(r"\s+", "", s)
    return s


def _prox_ocorrencia_anual(dt: date | None) -> date | None:
    if not dt:
        return None
    hoje = date.today()
    alvo = date(hoje.year, dt.month, dt.day)
    if alvo < hoje:
        alvo = date(hoje.year + 1, dt.month, dt.day)
    return alvo


def _build_docinfo(c) -> dict:
    today = date.today()
    cnh_ok = (c.cnh_validade is not None and c.cnh_validade >= today)
    placa_ok = (c.placa_validade is not None and c.placa_validade >= today)
    return {"cnh": {"ok": cnh_ok}, "placa": {"ok": placa_ok}}


def _abs_path_from_url(rel_url: str) -> str:
    if not rel_url:
        return ""
    if rel_url.startswith("/"):
        rel_url = rel_url.lstrip("/")
    return os.path.join(BASE_DIR, rel_url.replace("/", os.sep))


def _save_foto_to_db(entidade, file_storage, *, is_cooperado: bool) -> str | None:
    if not file_storage or not file_storage.filename:
        return getattr(entidade, "foto_url", None)

    data = file_storage.read()
    if not data:
        return getattr(entidade, "foto_url", None)

    entidade.foto_bytes = data
    entidade.foto_mime = file_storage.mimetype or "application/octet-stream"
    entidade.foto_filename = secure_filename(file_storage.filename)
    db.session.flush()

    if is_cooperado:
        url = url_for("media_coop", coop_id=entidade.id)
    else:
        url = url_for("media_rest", rest_id=entidade.id)

    entidade.foto_url = f"{url}?v={int(datetime.utcnow().timestamp())}"
    return entidade.foto_url


def salvar_documento_upload(file_storage) -> str | None:
    if not file_storage or not file_storage.filename:
        return None
    fname = secure_filename(file_storage.filename)
    base, ext = os.path.splitext(fname)
    unique = f"{base}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{ext.lower()}"
    destino = os.path.join(DOCS_PERSIST_DIR, unique)
    file_storage.save(destino)
    return unique


def resolve_documento_path(nome_arquivo: str) -> str | None:
    if not nome_arquivo:
        return None

    candidatos = [
        os.path.join(DOCS_PERSIST_DIR, nome_arquivo),
        os.path.join(DOCS_DIR, nome_arquivo),
        _abs_path_from_url(nome_arquivo) if str(nome_arquivo).startswith("/") else None,
    ]
    for p in candidatos:
        if p and os.path.isfile(p):
            return p
    return None


def salvar_tabela_upload(file_storage) -> str | None:
    if not file_storage or not file_storage.filename:
        return None
    fname = secure_filename(file_storage.filename)
    base, ext = os.path.splitext(fname)
    unique = f"{base}_{time.strftime('%Y%m%d_%H%M%S')}{ext.lower()}"
    destino = os.path.join(TABELAS_DIR, unique)
    file_storage.save(destino)
    return unique


def resolve_tabela_path(nome_arquivo: str) -> str | None:
    if not nome_arquivo:
        return None
    candidatos = [
        os.path.join(TABELAS_DIR, nome_arquivo),
        os.path.join(STATIC_TABLES, nome_arquivo),
        _abs_path_from_url(nome_arquivo) if nome_arquivo.startswith("/") else None,
    ]
    for p in candidatos:
        if p and os.path.isfile(p):
            return p
    return None

from __future__ import annotations

import os
import io
import re
import csv
import json
import time
import difflib
import mimetypes
import unicodedata
from pathlib import Path
from functools import wraps
from collections import defaultdict, namedtuple
from datetime import datetime, date, timedelta, time as dtime
from types import SimpleNamespace
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from dateutil.relativedelta import relativedelta

from flask import (
    Flask, render_template, request, redirect, url_for, session,
    flash, send_file, abort, jsonify, current_app
)
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

from sqlalchemy import (
    text, func, or_, and_, case, literal, inspect as sa_inspect, event
)
from sqlalchemy import delete as sa_delete
from sqlalchemy.engine import Engine
from sqlalchemy.exc import (
    OperationalError, SQLAlchemyError, IntegrityError,
    ProgrammingError, DisconnectionError
)
from sqlalchemy.pool import QueuePool

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment, PatternFill

mimetypes.add_type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx")
mimetypes.add_type("application/vnd.ms-excel", ".xls")
mimetypes.add_type("application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx")
mimetypes.add_type("application/msword", ".doc")
mimetypes.add_type("application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx")
mimetypes.add_type("application/vnd.ms-powerpoint", ".ppt")


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

UPLOAD_DIR = os.path.join(BASE_DIR, "static", "uploads")
DOCS_DIR = os.path.join(UPLOAD_DIR, "docs")
STATIC_TABLES = os.path.join(BASE_DIR, "static", "uploads", "tabelas")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(DOCS_DIR, exist_ok=True)
os.makedirs(STATIC_TABLES, exist_ok=True)

PERSIST_ROOT = os.environ.get("PERSIST_ROOT", "/var/data")
if not os.path.isdir(PERSIST_ROOT):
    PERSIST_ROOT = os.path.join(BASE_DIR, "data")
os.makedirs(PERSIST_ROOT, exist_ok=True)

DOCS_PERSIST_DIR = os.path.join(PERSIST_ROOT, "docs")
TABELAS_DIR = os.path.join(PERSIST_ROOT, "tabelas")
os.makedirs(DOCS_PERSIST_DIR, exist_ok=True)
os.makedirs(TABELAS_DIR, exist_ok=True)


def _merge_qs(url: str, extra: dict[str, str]) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    for k, v in (extra or {}).items():
        q.setdefault(k, v)
    return urlunparse(p._replace(query=urlencode(q, doseq=True)))


def _build_db_uri() -> str:
    raw = os.environ.get("DATABASE_URL")
    if not raw:
        return "sqlite:///" + os.path.join(BASE_DIR, "app.db")

    if raw.startswith("postgres://"):
        raw = raw.replace("postgres://", "postgresql+psycopg://", 1)
    elif raw.startswith("postgresql://") and "+psycopg" not in raw:
        raw = raw.replace("postgresql://", "postgresql+psycopg://", 1)

    extras = {
        "sslmode": "require",
        "keepalives": "1",
        "keepalives_idle": "30",
        "keepalives_interval": "10",
        "keepalives_count": "3",
        "application_name": os.environ.get("APP_NAME", "coopex"),
    }
    return _merge_qs(raw, extras)


app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = os.environ.get("SECRET_KEY", "coopex-secret")

URI = _build_db_uri()
if "sqlite" in URI and os.environ.get("FLASK_ENV") == "production":
    raise RuntimeError("DATABASE_URL ausente em produção")

workers = int(os.environ.get("WEB_CONCURRENCY", "1") or "1")
threads = int(os.environ.get("GTHREADS", "1") or "1")
req_concurrency = max(1, workers * threads)

target_total = int(os.environ.get("DB_TARGET_CONNS", "40") or "40")
per_worker_target = max(5, min(target_total // max(1, workers), 15))

default_pool_size = min(per_worker_target, req_concurrency + 2)
default_max_overflow = max(5, default_pool_size)

pool_size = int(os.environ.get("DB_POOL_SIZE", str(default_pool_size)))
max_overflow = int(os.environ.get("DB_MAX_OVERFLOW", str(default_max_overflow)))
pool_recycle = int(os.environ.get("SQL_POOL_RECYCLE", "240"))
pool_timeout = int(os.environ.get("SQL_POOL_TIMEOUT", "20"))

app.config.update(
    SQLALCHEMY_DATABASE_URI=URI,
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    JSON_SORT_KEYS=False,
    MAX_CONTENT_LENGTH=32 * 1024 * 1024,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("FLASK_SECURE_COOKIES", "1") == "1",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
    SQLALCHEMY_ENGINE_OPTIONS={
        "poolclass": QueuePool,
        "pool_size": pool_size,
        "max_overflow": max_overflow,
        "pool_timeout": pool_timeout,
        "pool_pre_ping": True,
        "pool_use_lifo": True,
        "pool_recycle": pool_recycle,
        "connect_args": {
            "connect_timeout": int(os.getenv("PGCONNECT_TIMEOUT", "5")),
            "options": "-c statement_timeout=15000",
        },
    },
)

db = SQLAlchemy(app)


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.get("/readyz")
def readyz():
    try:
        db.session.execute(text("SELECT 1"))
        return "ready", 200
    except Exception:
        return "not-ready", 503


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_con, con_record):
    try:
        if "sqlite" in app.config["SQLALCHEMY_DATABASE_URI"]:
            cur = dbapi_con.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()
    except Exception:
        pass


def _is_sqlite() -> bool:
    try:
        return db.session.get_bind().dialect.name == "sqlite"
    except Exception:
        return "sqlite" in (app.config.get("SQLALCHEMY_DATABASE_URI") or "")


INSS_ALIQ = float(os.environ.get("ALIQUOTA_INSS", "0.04"))
SEST_ALIQ = float(os.environ.get("ALIQUOTA_SEST", "0.005"))


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


def _fmt_time(t) -> str:
    if not t:
        return ""
    if hasattr(t, "strftime"):
        try:
            return t.strftime("%H:%M")
        except Exception:
            pass
    return str(t)


def _dow(d: date) -> str:
    return str(d.weekday()) if d else ""


def role_required(role: str):
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if session.get("user_tipo") != role:
                return redirect(url_for("login"))
            return fn(*args, **kwargs)
        return wrapper
    return deco


def admin_required(fn):
    return role_required("admin")(fn)


def _normalize_name(s: str) -> list[str]:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s)
    return [p.lower() for p in s.split() if p.strip()]


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", str(s or "").strip().lower())
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


def _norm_login(s: str) -> str:
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower().strip()
    s = re.sub(r"\s+", "", s)
    return s


def _prox_ocorrencia_anual(dt: date | None) -> date | None:
    if not dt:
        return None
    hoje = date.today()
    alvo = date(hoje.year, dt.month, dt.day)
    if alvo < hoje:
        alvo = date(hoje.year + 1, dt.month, dt.day)
    return alvo


def _build_docinfo(c) -> dict:
    today = date.today()
    cnh_ok = (c.cnh_validade is not None and c.cnh_validade >= today)
    placa_ok = (c.placa_validade is not None and c.placa_validade >= today)
    return {"cnh": {"ok": cnh_ok}, "placa": {"ok": placa_ok}}


def _abs_path_from_url(rel_url: str) -> str:
    if not rel_url:
        return ""
    if rel_url.startswith("/"):
        rel_url = rel_url.lstrip("/")
    return os.path.join(BASE_DIR, rel_url.replace("/", os.sep))


def _save_foto_to_db(entidade, file_storage, *, is_cooperado: bool) -> str | None:
    if not file_storage or not file_storage.filename:
        return getattr(entidade, "foto_url", None)

    data = file_storage.read()
    if not data:
        return getattr(entidade, "foto_url", None)

    entidade.foto_bytes = data
    entidade.foto_mime = file_storage.mimetype or "application/octet-stream"
    entidade.foto_filename = secure_filename(file_storage.filename)
    db.session.flush()

    if is_cooperado:
        url = url_for("media_coop", coop_id=entidade.id)
    else:
        url = url_for("media_rest", rest_id=entidade.id)

    entidade.foto_url = f"{url}?v={int(datetime.utcnow().timestamp())}"
    return entidade.foto_url


def salvar_documento_upload(file_storage) -> str | None:
    if not file_storage or not file_storage.filename:
        return None
    fname = secure_filename(file_storage.filename)
    base, ext = os.path.splitext(fname)
    unique = f"{base}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{ext.lower()}"
    destino = os.path.join(DOCS_PERSIST_DIR, unique)
    file_storage.save(destino)
    return unique


def resolve_documento_path(nome_arquivo: str) -> str | None:
    if not nome_arquivo:
        return None

    candidatos = [
        os.path.join(DOCS_PERSIST_DIR, nome_arquivo),
        os.path.join(DOCS_DIR, nome_arquivo),
        _abs_path_from_url(nome_arquivo) if str(nome_arquivo).startswith("/") else None,
    ]
    for p in candidatos:
        if p and os.path.isfile(p):
            return p
    return None


def salvar_tabela_upload(file_storage) -> str | None:
    if not file_storage or not file_storage.filename:
        return None
    fname = secure_filename(file_storage.filename)
    base, ext = os.path.splitext(fname)
    unique = f"{base}_{time.strftime('%Y%m%d_%H%M%S')}{ext.lower()}"
    destino = os.path.join(TABELAS_DIR, unique)
    file_storage.save(destino)
    return unique


def resolve_tabela_path(nome_arquivo: str) -> str | None:
    if not nome_arquivo:
        return None
    candidatos = [
        os.path.join(TABELAS_DIR, nome_arquivo),
        os.path.join(STATIC_TABLES, nome_arquivo),
        _abs_path_from_url(nome_arquivo) if nome_arquivo.startswith("/") else None,
    ]
    for p in candidatos:
        if p and os.path.isfile(p):
            return p
    return None

def _admin_dashboard_context(active_tab="resumo"):
    import time
    t0 = time.time()
    args = request.args

    def _pick_date(*keys):
        for k in keys:
            v = args.get(k)
            if v:
                d = _parse_date(v)
                if d:
                    return d
        return None

    data_inicio = _pick_date("resumo_inicio", "data_inicio")
    data_fim = _pick_date("resumo_fim", "data_fim")

    restaurante_id = args.get("restaurante_id", type=int)
    cooperado_id = args.get("cooperado_id", type=int)
    considerar_periodo = bool(args.get("considerar_periodo"))
    dows = set(args.getlist("dow"))

    q = Lancamento.query

    if restaurante_id:
        q = q.filter(Lancamento.restaurante_id == restaurante_id)
    if cooperado_id:
        q = q.filter(Lancamento.cooperado_id == cooperado_id)
    if data_inicio:
        q = q.filter(Lancamento.data >= data_inicio)
    if data_fim:
        q = q.filter(Lancamento.data <= data_fim)

    if dows:
        try:
            dows_int = [int(x) for x in dows if str(x).isdigit()]
        except Exception:
            dows_int = []

        if dows_int:
            dow_map = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 0}
            db_dows = [dow_map[d] for d in dows_int if d in dow_map]
            if db_dows:
                q = q.filter(db.extract("dow", Lancamento.data).in_(db_dows))

    lancamentos = q.order_by(Lancamento.data.desc(), Lancamento.id.desc()).all()

    if considerar_periodo and restaurante_id:
        rest = Restaurante.query.get(restaurante_id)
        if rest:
            mapa = {
                "seg-dom": {"1", "2", "3", "4", "5", "6", "7"},
                "sab-sex": {"6", "7", "1", "2", "3", "4", "5"},
                "sex-qui": {"5", "6", "7", "1", "2", "3", "4"},
            }
            permitidos = mapa.get(rest.periodo, {"1", "2", "3", "4", "5", "6", "7"})
            lancamentos = [l for l in lancamentos if l.data and _dow(l.data) in permitidos]

    total_producoes = sum((l.valor or 0.0) for l in lancamentos)
    total_inss = round(total_producoes * INSS_ALIQ, 2)
    total_sest = round(total_producoes * SEST_ALIQ, 2)
    total_encargos = round(total_inss + total_sest, 2)

    rq = ReceitaCooperativa.query
    dq = DespesaCooperativa.query

    if data_inicio:
        rq = rq.filter(ReceitaCooperativa.data >= data_inicio)
        dq = dq.filter(DespesaCooperativa.data >= data_inicio)
    if data_fim:
        rq = rq.filter(ReceitaCooperativa.data <= data_fim)
        dq = dq.filter(DespesaCooperativa.data <= data_fim)

    receitas = rq.order_by(ReceitaCooperativa.data.desc().nullslast(), ReceitaCooperativa.id.desc()).all()
    despesas = dq.order_by(DespesaCooperativa.data.desc(), DespesaCooperativa.id.desc()).all()

    total_receitas = sum((r.valor_total or 0.0) for r in receitas)
    total_despesas = sum((d.valor or 0.0) for d in despesas)

    rq2 = ReceitaCooperado.query
    dq2 = DespesaCooperado.query

    if data_inicio:
        rq2 = rq2.filter(ReceitaCooperado.data >= data_inicio)
    if data_fim:
        rq2 = rq2.filter(ReceitaCooperado.data <= data_fim)

    if data_inicio and data_fim:
        dq2 = dq2.filter(DespesaCooperado.data_inicio <= data_fim, DespesaCooperado.data_fim >= data_inicio)
    elif data_inicio:
        dq2 = dq2.filter(DespesaCooperado.data_fim >= data_inicio)
    elif data_fim:
        dq2 = dq2.filter(DespesaCooperado.data_inicio <= data_fim)

    receitas_coop = rq2.order_by(ReceitaCooperado.data.desc(), ReceitaCooperado.id.desc()).all()
    despesas_coop = dq2.order_by(DespesaCooperado.data_fim.desc().nullslast(), DespesaCooperado.id.desc()).all()

    total_receitas_coop = sum((r.valor or 0.0) for r in receitas_coop)
    total_despesas_coop = sum((d.valor or 0.0) for d in despesas_coop if not d.eh_adiantamento)
    total_adiantamentos_coop = sum((d.valor or 0.0) for d in despesas_coop if d.eh_adiantamento)

    cfg = get_config()
    cooperados = Cooperado.query.order_by(Cooperado.nome).all()
    restaurantes = Restaurante.query.order_by(Restaurante.nome).all()

    docinfo_map = {c.id: _build_docinfo(c) for c in cooperados}
    status_doc_por_coop = {
        c.id: {
            "cnh_ok": docinfo_map[c.id]["cnh"]["ok"],
            "placa_ok": docinfo_map[c.id]["placa"]["ok"],
        }
        for c in cooperados
    }

    escalas_all = Escala.query.order_by(Escala.id.asc()).all()
    esc_by_int = defaultdict(list)
    esc_by_str = defaultdict(list)

    for e in escalas_all:
        k_int = e.cooperado_id if e.cooperado_id is not None else 0
        esc_item = {
            "data": e.data,
            "turno": e.turno,
            "horario": e.horario,
            "contrato": e.contrato,
            "cor": e.cor,
            "nome_planilha": e.cooperado_nome,
        }
        esc_by_int[k_int].append(esc_item)
        esc_by_str[str(k_int)].append(esc_item)

    cont_rows = dict(
        db.session.query(Escala.cooperado_id, func.count(Escala.id))
        .group_by(Escala.cooperado_id)
        .all()
    )
    qtd_escalas_map = {c.id: int(cont_rows.get(c.id, 0)) for c in cooperados}
    qtd_sem_cadastro = int(cont_rows.get(None, 0))

    sums = {}
    for l in lancamentos:
        if not l.data:
            continue
        key = l.data.strftime("%Y-%m")
        sums[key] = sums.get(key, 0.0) + (l.valor or 0.0)

    labels_ord = sorted(sums.keys())

    def _fmt_label(k: str) -> str:
        parts = k.split("-")
        if len(parts) == 2 and parts[0] and parts[1]:
            year, month = parts[0], parts[1]
            return f"{month}/{year[-2:]}"
        return k

    labels_fmt = [_fmt_label(k) for k in labels_ord]
    values = [round(sums[k], 2) for k in labels_ord]
    chart_data_lancamentos_coop = {"labels": labels_fmt, "values": values}
    chart_data_lancamentos_cooperados = chart_data_lancamentos_coop

    admin_user = Usuario.query.filter_by(tipo="admin").first()

    print("TEMPO ADMIN:", time.time() - t0)

    return {
        "aba_ativa": active_tab,
        "tab": active_tab,
        "total_producoes": total_producoes,
        "total_inss": total_inss,
        "total_sest": total_sest,
        "total_encargos": total_encargos,
        "total_receitas": total_receitas,
        "total_despesas": total_despesas,
        "total_receitas_coop": total_receitas_coop,
        "total_despesas_coop": total_despesas_coop,
        "total_adiantamentos_coop": total_adiantamentos_coop,
        "salario_minimo": cfg.salario_minimo or 0.0,
        "lancamentos": lancamentos,
        "receitas": receitas,
        "despesas": despesas,
        "receitas_coop": receitas_coop,
        "despesas_coop": despesas_coop,
        "cooperados": cooperados,
        "restaurantes": restaurantes,
        "current_date": date.today(),
        "data_limite": date(date.today().year, 12, 31),
        "admin": admin_user,
        "docinfo_map": docinfo_map,
        "escalas_por_coop": esc_by_int,
        "escalas_por_coop_json": esc_by_str,
        "qtd_escalas_map": qtd_escalas_map,
        "qtd_escalas_sem_cadastro": qtd_sem_cadastro,
        "status_doc_por_coop": status_doc_por_coop,
        "chart_data_lancamentos_coop": chart_data_lancamentos_coop,
        "chart_data_lancamentos_cooperados": chart_data_lancamentos_cooperados,
    }


@app.route("/admin", methods=["GET"])
@admin_required
def admin_dashboard():
    return redirect(url_for("admin_resumo_split"))


@app.get("/admin/resumo")
@admin_required
def admin_resumo_split():
    return render_template("resumo.html", **_admin_dashboard_context("resumo"))


@app.get("/admin/lancamentos")
@admin_required
def admin_lancamentos_split():
    return render_template("lancamentos.html", **_admin_dashboard_context("lancamentos"))


@app.get("/admin/receitas")
@admin_required
def admin_receitas_split():
    return render_template("receitas.html", **_admin_dashboard_context("receitas"))


@app.get("/admin/despesas")
@admin_required
def admin_despesas_split():
    return render_template("despesas.html", **_admin_dashboard_context("despesas"))


@app.get("/admin/coop_receitas")
@admin_required
def admin_coop_receitas_split():
    return render_template("coop_receitas.html", **_admin_dashboard_context("coop_receitas"))


@app.get("/admin/coop_despesas")
@admin_required
def admin_coop_despesas_split():
    return render_template("coop_despesas.html", **_admin_dashboard_context("coop_despesas"))


@app.get("/admin/beneficios")
@admin_required
def admin_beneficios_split():
    return render_template("beneficios.html", **_admin_dashboard_context("beneficios"))


@app.get("/admin/cooperados")
@admin_required
def admin_cooperados_split():
    return render_template("cooperados.html", **_admin_dashboard_context("cooperados"))


@app.get("/admin/restaurantes")
@admin_required
def admin_restaurantes_split():
    return render_template("restaurantes.html", **_admin_dashboard_context("restaurantes"))


@app.get("/admin/escalas")
@admin_required
def admin_escalas_split():
    return render_template("escalas.html", **_admin_dashboard_context("escalas"))


@app.get("/admin/config")
@admin_required
def admin_config_split():
    return render_template("config.html", **_admin_dashboard_context("config"))


@app.route("/config/update", methods=["POST"])
@admin_required
def update_config():
    cfg = get_config()
    cfg.salario_minimo = request.form.get("salario_minimo", type=float) or 0.0
    db.session.commit()
    flash("Configuração atualizada.", "success")
    return redirect(url_for("admin_config_split"))


@app.route("/admin/alterar_admin", methods=["POST"])
@admin_required
def alterar_admin():
    admin = Usuario.query.filter_by(tipo="admin").first()
    if not admin:
        flash("Administrador não encontrado.", "danger")
        return redirect(url_for("admin_config_split"))

    admin.usuario = (request.form.get("usuario") or admin.usuario).strip()
    nova = request.form.get("nova_senha", "")
    confirmar = request.form.get("confirmar_senha", "")

    if nova or confirmar:
        if nova != confirmar:
            flash("As senhas não conferem.", "warning")
            return redirect(url_for("admin_config_split"))
        admin.set_password(nova)

    db.session.commit()
    flash("Conta do administrador atualizada.", "success")
    return redirect(url_for("admin_config_split"))


@app.post("/admin/cooperados/<int:id>/toggle-status")
@admin_required
def toggle_status_cooperado(id):
    try:
        coop = db.session.get(Cooperado, id)
        if not coop or not getattr(coop, "usuario_ref", None):
            return jsonify(ok=False, error="Cooperado não encontrado"), 404

        user = coop.usuario_ref
        user.ativo = not bool(getattr(user, "ativo", True))
        db.session.commit()
        return jsonify(ok=True, ativo=bool(user.ativo))
    except SQLAlchemyError:
        db.session.rollback()
        return jsonify(ok=False, error="Falha ao salvar no banco"), 500


@app.route("/cooperados/add", methods=["POST"])
@admin_required
def add_cooperado():
    f = request.form
    nome = (f.get("nome") or "").strip()
    usuario_login = (f.get("usuario") or "").strip()
    senha = f.get("senha") or ""
    telefone = (f.get("telefone") or "").strip()
    foto = request.files.get("foto")

    if Usuario.query.filter_by(usuario=usuario_login).first():
        flash("Usuário já existente.", "warning")
        return redirect(url_for("admin_cooperados_split"))

    u = Usuario(usuario=usuario_login, tipo="cooperado", senha_hash="")
    u.set_password(senha)
    db.session.add(u)
    db.session.flush()

    c = Cooperado(nome=nome, usuario_id=u.id, telefone=telefone, ultima_atualizacao=datetime.now())
    db.session.add(c)
    db.session.flush()

    if foto and foto.filename:
        _save_foto_to_db(c, foto, is_cooperado=True)

    db.session.commit()
    flash("Cooperado cadastrado.", "success")
    return redirect(url_for("admin_cooperados_split"))


@app.route("/cooperados/<int:id>/edit", methods=["POST"])
@admin_required
def edit_cooperado(id):
    c = Cooperado.query.get_or_404(id)
    f = request.form

    c.nome = (f.get("nome") or "").strip()
    c.usuario_ref.usuario = (f.get("usuario") or "").strip()
    c.telefone = (f.get("telefone") or "").strip()

    foto = request.files.get("foto")
    if foto and foto.filename:
        _save_foto_to_db(c, foto, is_cooperado=True)

    c.ultima_atualizacao = datetime.now()
    db.session.commit()
    flash("Cooperado atualizado.", "success")
    return redirect(url_for("admin_cooperados_split"))


@app.route("/cooperados/<int:id>/delete", methods=["POST"])
@admin_required
def delete_cooperado(id):
    c = Cooperado.query.get_or_404(id)
    u = c.usuario_ref

    try:
        db.session.execute(sa_delete(Lancamento).where(Lancamento.cooperado_id == id))
        db.session.execute(sa_delete(ReceitaCooperado).where(ReceitaCooperado.cooperado_id == id))
        db.session.execute(sa_delete(DespesaCooperado).where(DespesaCooperado.cooperado_id == id))
        db.session.execute(sa_delete(AvaliacaoCooperado).where(AvaliacaoCooperado.cooperado_id == id))
        db.session.execute(sa_delete(AvaliacaoRestaurante).where(AvaliacaoRestaurante.cooperado_id == id))
        db.session.execute(sa_delete(TrocaSolicitacao).where(or_(
            TrocaSolicitacao.solicitante_id == id,
            TrocaSolicitacao.destino_id == id
        )))
        db.session.execute(sa_delete(Escala).where(Escala.cooperado_id == id))

        db.session.delete(c)
        if u:
            db.session.delete(u)

        db.session.commit()
        flash("Cooperado excluído.", "success")
    except IntegrityError as e:
        db.session.rollback()
        current_app.logger.exception(e)
        flash("Não foi possível excluir: existem vínculos ativos.", "danger")

    return redirect(url_for("admin_cooperados_split"))


@app.route("/cooperados/<int:id>/reset_senha", methods=["POST"])
@admin_required
def reset_senha_cooperado(id):
    c = Cooperado.query.get_or_404(id)
    ns = request.form.get("nova_senha") or ""
    cs = request.form.get("confirmar_senha") or ""

    if ns != cs:
        flash("As senhas não conferem.", "warning")
        return redirect(url_for("admin_cooperados_split"))

    c.usuario_ref.set_password(ns)
    db.session.commit()
    flash("Senha do cooperado atualizada.", "success")
    return redirect(url_for("admin_cooperados_split"))


@app.route("/restaurantes/add", methods=["POST"])
@admin_required
def add_restaurante():
    f = request.form
    nome = (f.get("nome") or "").strip()
    periodo = f.get("periodo", "seg-dom")
    usuario_login = (f.get("usuario") or "").strip()
    senha = f.get("senha", "")
    foto = request.files.get("foto")

    if Usuario.query.filter_by(usuario=usuario_login).first():
        flash("Usuário já existente.", "warning")
        return redirect(url_for("admin_restaurantes_split"))

    u = Usuario(usuario=usuario_login, tipo="restaurante", senha_hash="")
    u.set_password(senha)
    db.session.add(u)
    db.session.flush()

    r = Restaurante(nome=nome, periodo=periodo, usuario_id=u.id)
    db.session.add(r)
    db.session.flush()

    if foto and foto.filename:
        _save_foto_to_db(r, foto, is_cooperado=False)

    db.session.commit()
    flash("Estabelecimento cadastrado.", "success")
    return redirect(url_for("admin_restaurantes_split"))


@app.route("/restaurantes/<int:id>/edit", methods=["POST"])
@admin_required
def edit_restaurante(id):
    r = Restaurante.query.get_or_404(id)
    f = request.form

    r.nome = (f.get("nome") or "").strip()
    r.periodo = f.get("periodo", "seg-dom")
    r.usuario_ref.usuario = (f.get("usuario") or "").strip()

    foto = request.files.get("foto")
    if foto and foto.filename:
        _save_foto_to_db(r, foto, is_cooperado=False)

    db.session.commit()
    flash("Estabelecimento atualizado.", "success")
    return redirect(url_for("admin_restaurantes_split"))


@app.route("/restaurantes/<int:id>/delete", methods=["POST"])
@admin_required
def delete_restaurante(id):
    r = Restaurante.query.get_or_404(id)
    u = r.usuario_ref

    try:
        db.session.execute(sa_delete(Lancamento).where(Lancamento.restaurante_id == id))
        db.session.execute(sa_delete(Escala).where(Escala.restaurante_id == id))

        db.session.delete(r)
        if u:
            db.session.delete(u)

        db.session.commit()
        flash("Estabelecimento excluído.", "success")
    except IntegrityError as e:
        db.session.rollback()
        current_app.logger.exception(e)
        flash("Não foi possível excluir: existem vínculos ativos.", "danger")

    return redirect(url_for("admin_restaurantes_split"))


@app.route("/restaurantes/<int:id>/reset_senha", methods=["POST"])
@admin_required
def reset_senha_restaurante(id):
    r = Restaurante.query.get_or_404(id)
    ns = request.form.get("nova_senha") or ""
    cs = request.form.get("confirmar_senha") or ""

    if ns != cs:
        flash("As senhas não conferem.", "warning")
        return redirect(url_for("admin_restaurantes_split"))

    r.usuario_ref.set_password(ns)
    db.session.commit()
    flash("Senha do restaurante atualizada.", "success")
    return redirect(url_for("admin_restaurantes_split"))


@app.route("/documentos/<int:coop_id>", methods=["GET", "POST"])
@admin_required
def editar_documentos(coop_id):
    cooperado = Cooperado.query.get_or_404(coop_id)

    def parse_date_local(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date() if s else None
        except Exception:
            return None

    if request.method == "POST":
        f = request.form
        cooperado.cnh_numero = f.get("cnh_numero")
        cooperado.placa = f.get("placa")
        cooperado.cnh_validade = parse_date_local(f.get("cnh_validade"))
        cooperado.placa_validade = parse_date_local(f.get("placa_validade"))
        cooperado.ultima_atualizacao = datetime.now()
        db.session.commit()
        flash("Documentos atualizados.", "success")
        return redirect(url_for("admin_escalas_split"))

    hoje = date.today()
    prazo_final = date(hoje.year, 12, 31)

    docinfo = {
        "prazo_final": prazo_final,
        "dias_ate_prazo": max(0, (prazo_final - hoje).days),
        "cnh": {
            "numero": cooperado.cnh_numero,
            "validade": cooperado.cnh_validade,
            "prox_validade": _prox_ocorrencia_anual(cooperado.cnh_validade),
            "ok": cooperado.cnh_validade is not None and cooperado.cnh_validade >= hoje,
            "modo": "auto",
        },
        "placa": {
            "numero": cooperado.placa,
            "validade": cooperado.placa_validade,
            "prox_validade": _prox_ocorrencia_anual(cooperado.placa_validade),
            "ok": cooperado.placa_validade is not None and cooperado.placa_validade >= hoje,
            "modo": "auto",
        },
    }

    return render_template("editar_tabelas.html", cooperado=cooperado, coop=cooperado, c=cooperado, docinfo=docinfo)


@app.route("/receitas/add", methods=["POST"])
@admin_required
def add_receita():
    f = request.form
    obj = ReceitaCooperativa(
        descricao=(f.get("descricao") or "").strip(),
        valor_total=f.get("valor", type=float) or 0.0,
        data=_parse_date(f.get("data")),
    )
    db.session.add(obj)
    db.session.commit()
    flash("Receita adicionada.", "success")
    return redirect(url_for("admin_receitas_split"))


@app.route("/receitas/<int:id>/edit", methods=["POST"])
@admin_required
def edit_receita(id):
    obj = ReceitaCooperativa.query.get_or_404(id)
    f = request.form
    obj.descricao = (f.get("descricao") or "").strip()
    obj.valor_total = f.get("valor", type=float) or 0.0
    obj.data = _parse_date(f.get("data"))
    db.session.commit()
    flash("Receita atualizada.", "success")
    return redirect(url_for("admin_receitas_split"))


@app.route("/receitas/<int:id>/delete", methods=["POST"])
@admin_required
def delete_receita(id):
    obj = ReceitaCooperativa.query.get_or_404(id)
    db.session.delete(obj)
    db.session.commit()
    flash("Receita excluída.", "success")
    return redirect(url_for("admin_receitas_split"))


@app.route("/despesas/add", methods=["POST"])
@admin_required
def add_despesa():
    f = request.form
    obj = DespesaCooperativa(
        descricao=(f.get("descricao") or "").strip(),
        valor=f.get("valor", type=float) or 0.0,
        data=_parse_date(f.get("data")),
    )
    db.session.add(obj)
    db.session.commit()
    flash("Despesa adicionada.", "success")
    return redirect(url_for("admin_despesas_split"))


@app.route("/despesas/<int:id>/edit", methods=["POST"])
@admin_required
def edit_despesa(id):
    obj = DespesaCooperativa.query.get_or_404(id)
    f = request.form
    obj.descricao = (f.get("descricao") or "").strip()
    obj.valor = f.get("valor", type=float) or 0.0
    obj.data = _parse_date(f.get("data"))
    db.session.commit()
    flash("Despesa atualizada.", "success")
    return redirect(url_for("admin_despesas_split"))


@app.route("/despesas/<int:id>/delete", methods=["POST"])
@admin_required
def delete_despesa(id):
    obj = DespesaCooperativa.query.get_or_404(id)
    db.session.delete(obj)
    db.session.commit()
    flash("Despesa excluída.", "success")
    return redirect(url_for("admin_despesas_split"))


@app.route("/coop_receitas/add", methods=["POST"])
@admin_required
def add_receita_coop():
    f = request.form
    obj = ReceitaCooperado(
        cooperado_id=f.get("cooperado_id", type=int),
        descricao=(f.get("descricao") or "").strip(),
        valor=f.get("valor", type=float) or 0.0,
        data=_parse_date(f.get("data")),
    )
    db.session.add(obj)
    db.session.commit()
    flash("Receita do cooperado adicionada.", "success")
    return redirect(url_for("admin_coop_receitas_split"))


@app.route("/coop_receitas/<int:id>/edit", methods=["POST"])
@admin_required
def edit_receita_coop(id):
    obj = ReceitaCooperado.query.get_or_404(id)
    f = request.form
    obj.cooperado_id = f.get("cooperado_id", type=int)
    obj.descricao = (f.get("descricao") or "").strip()
    obj.valor = f.get("valor", type=float) or 0.0
    obj.data = _parse_date(f.get("data"))
    db.session.commit()
    flash("Receita do cooperado atualizada.", "success")
    return redirect(url_for("admin_coop_receitas_split"))


@app.route("/coop_receitas/<int:id>/delete", methods=["POST"])
@admin_required
def delete_receita_coop(id):
    obj = ReceitaCooperado.query.get_or_404(id)
    db.session.delete(obj)
    db.session.commit()
    flash("Receita do cooperado excluída.", "success")
    return redirect(url_for("admin_coop_receitas_split"))


@app.route("/coop_despesas/add", methods=["POST"])
@admin_required
def add_despesa_coop():
    f = request.form
    obj = DespesaCooperado(
        cooperado_id=f.get("cooperado_id", type=int),
        descricao=(f.get("descricao") or "").strip(),
        valor=f.get("valor", type=float) or 0.0,
        data=_parse_date(f.get("data")),
        data_inicio=_parse_date(f.get("data_inicio") or f.get("data")),
        data_fim=_parse_date(f.get("data_fim") or f.get("data")),
        eh_adiantamento=bool(f.get("eh_adiantamento")),
    )
    db.session.add(obj)
    db.session.commit()
    flash("Despesa do cooperado adicionada.", "success")
    return redirect(url_for("admin_coop_despesas_split"))


@app.route("/coop_despesas/<int:id>/edit", methods=["POST"])
@admin_required
def edit_despesa_coop(id):
    obj = DespesaCooperado.query.get_or_404(id)
    f = request.form
    obj.cooperado_id = f.get("cooperado_id", type=int)
    obj.descricao = (f.get("descricao") or "").strip()
    obj.valor = f.get("valor", type=float) or 0.0
    obj.data = _parse_date(f.get("data"))
    obj.data_inicio = _parse_date(f.get("data_inicio") or f.get("data"))
    obj.data_fim = _parse_date(f.get("data_fim") or f.get("data"))
    obj.eh_adiantamento = bool(f.get("eh_adiantamento"))
    db.session.commit()
    flash("Despesa do cooperado atualizada.", "success")
    return redirect(url_for("admin_coop_despesas_split"))


@app.route("/coop_despesas/<int:id>/delete", methods=["POST"])
@admin_required
def delete_despesa_coop(id):
    obj = DespesaCooperado.query.get_or_404(id)
    db.session.delete(obj)
    db.session.commit()
    flash("Despesa do cooperado excluída.", "success")
    return redirect(url_for("admin_coop_despesas_split"))


@app.route("/admin/lancamentos/add", methods=["POST"])
@admin_required
def admin_add_lancamento():
    f = request.form
    l = Lancamento(
        restaurante_id=f.get("restaurante_id", type=int),
        cooperado_id=f.get("cooperado_id", type=int),
        descricao=(f.get("descricao") or "").strip(),
        valor=f.get("valor", type=float) or 0.0,
        data=_parse_date(f.get("data")),
        hora_inicio=f.get("hora_inicio"),
        hora_fim=f.get("hora_fim"),
        qtd_entregas=f.get("qtd_entregas", type=int),
    )
    db.session.add(l)
    db.session.commit()
    flash("Lançamento inserido.", "success")
    return redirect(url_for("admin_lancamentos_split"))


@app.route("/admin/lancamentos/<int:id>/edit", methods=["POST"])
@admin_required
def admin_edit_lancamento(id):
    l = Lancamento.query.get_or_404(id)
    f = request.form
    l.restaurante_id = f.get("restaurante_id", type=int)
    l.cooperado_id = f.get("cooperado_id", type=int)
    l.descricao = (f.get("descricao") or "").strip()
    l.valor = f.get("valor", type=float) or 0.0
    l.data = _parse_date(f.get("data"))
    l.hora_inicio = f.get("hora_inicio")
    l.hora_fim = f.get("hora_fim")
    l.qtd_entregas = f.get("qtd_entregas", type=int)
    db.session.commit()
    flash("Lançamento atualizado.", "success")
    return redirect(url_for("admin_lancamentos_split"))


@app.route("/admin/lancamentos/<int:id>/delete", methods=["POST"])
@admin_required
def admin_delete_lancamento(id):
    l = Lancamento.query.get_or_404(id)
    db.session.execute(sa_delete(AvaliacaoCooperado).where(AvaliacaoCooperado.lancamento_id == id))
    db.session.execute(sa_delete(AvaliacaoRestaurante).where(AvaliacaoRestaurante.lancamento_id == id))
    db.session.delete(l)
    db.session.commit()
    flash("Lançamento excluído.", "success")
    return redirect(url_for("admin_lancamentos_split"))


@app.post("/restaurante/lancar_producao")
@role_required("restaurante")
def lancar_producao():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    if not rest:
        abort(403)

    f = request.form
    l = Lancamento(
        restaurante_id=rest.id,
        cooperado_id=f.get("cooperado_id", type=int),
        descricao=((f.get("descricao") or "").strip() or None),
        valor=f.get("valor", type=float) or 0.0,
        data=_parse_date(f.get("data")) or date.today(),
        hora_inicio=f.get("hora_inicio"),
        hora_fim=f.get("hora_fim"),
        qtd_entregas=f.get("qtd_entregas", type=int),
    )
    db.session.add(l)
    db.session.commit()

    flash("Produção lançada.", "success")
    return redirect(url_for("portal_restaurante", view="lancar"))


@app.route("/lancamentos/<int:id>/editar", methods=["GET", "POST"])
@role_required("restaurante")
def editar_lancamento(id):
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    l = Lancamento.query.get_or_404(id)

    if not rest or l.restaurante_id != rest.id:
        abort(403)

    if request.method == "POST":
        f = request.form
        l.valor = f.get("valor", type=float) or 0.0
        l.data = _parse_date(f.get("data")) or l.data
        l.hora_inicio = f.get("hora_inicio")
        l.hora_fim = f.get("hora_fim")
        l.qtd_entregas = f.get("qtd_entregas", type=int)
        l.descricao = (f.get("descricao") or "").strip() or None
        db.session.commit()
        flash("Lançamento atualizado.", "success")
        return redirect(url_for("portal_restaurante", view="lancamentos"))

    return render_template("editar_lancamento.html", lanc=l)


@app.get("/lancamentos/<int:id>/excluir")
@role_required("restaurante")
def excluir_lancamento(id):
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    l = Lancamento.query.get_or_404(id)

    if not rest or l.restaurante_id != rest.id:
        abort(403)

    db.session.execute(sa_delete(AvaliacaoCooperado).where(AvaliacaoCooperado.lancamento_id == id))
    db.session.execute(sa_delete(AvaliacaoRestaurante).where(AvaliacaoRestaurante.lancamento_id == id))
    db.session.delete(l)
    db.session.commit()
    flash("Lançamento excluído.", "success")
    return redirect(url_for("portal_restaurante", view="lancamentos"))


@app.route("/admin/avaliacoes", methods=["GET"])
@admin_required
def admin_avaliacoes():
    tipo_raw = (request.args.get("tipo") or "cooperado").strip().lower()
    tipo = "restaurante" if tipo_raw == "restaurante" else "cooperado"

    restaurante_id = request.args.get("restaurante_id", type=int)
    cooperado_id = request.args.get("cooperado_id", type=int)
    data_inicio = (request.args.get("data_inicio") or "").strip()
    data_fim = (request.args.get("data_fim") or "").strip()

    di = _parse_date(data_inicio)
    df = _parse_date(data_fim)

    Model = AvaliacaoRestaurante if tipo == "restaurante" else AvaliacaoCooperado

    base = (
        db.session.query(
            Model,
            Restaurante.id.label("rest_id"),
            Restaurante.nome.label("rest_nome"),
            Cooperado.id.label("coop_id"),
            Cooperado.nome.label("coop_nome"),
        )
        .join(Restaurante, Model.restaurante_id == Restaurante.id)
        .join(Cooperado, Model.cooperado_id == Cooperado.id)
    )

    filtros = []
    if restaurante_id:
        filtros.append(Model.restaurante_id == restaurante_id)
    if cooperado_id:
        filtros.append(Model.cooperado_id == cooperado_id)
    if di:
        filtros.append(func.date(Model.criado_em) >= di)
    if df:
        filtros.append(func.date(Model.criado_em) <= df)

    if filtros:
        base = base.filter(and_(*filtros))

    total = base.with_entities(func.count(Model.id)).scalar() or 0
    rows = base.order_by(Model.criado_em.desc()).all()

    avaliacoes = []
    for a, rest_id, rest_nome, coop_id, coop_nome in rows:
        avaliacoes.append(SimpleNamespace(
            criado_em=a.criado_em,
            rest_id=rest_id,
            rest_nome=rest_nome,
            coop_id=coop_id,
            coop_nome=coop_nome,
            geral=getattr(a, "estrelas_geral", 0) or 0,
            comentario=(getattr(a, "comentario", "") or "").strip(),
            media=getattr(a, "media_ponderada", None),
            sentimento=getattr(a, "sentimento", None),
            temas=getattr(a, "temas", None),
            alerta=bool(getattr(a, "alerta_crise", False)),
        ))

    cfg = get_config()
    admin_user = Usuario.query.filter_by(tipo="admin").first()

    return render_template(
        "admin_avaliacoes.html",
        aba_ativa="avaliacoes",
        tab="avaliacoes",
        tipo=tipo,
        avaliacoes=avaliacoes,
        kpis={"qtd": total, "geral": 0},
        ranking=[],
        chart_top={"labels": [], "values": []},
        compat=[],
        _flt=SimpleNamespace(
            restaurante_id=restaurante_id,
            cooperado_id=cooperado_id,
            data_inicio=data_inicio or "",
            data_fim=data_fim or "",
        ),
        restaurantes=Restaurante.query.order_by(Restaurante.nome).all(),
        cooperados=Cooperado.query.order_by(Cooperado.nome).all(),
        pager=SimpleNamespace(page=1, per_page=100, total=total, pages=1, has_prev=False, has_next=False),
        page=1,
        per_page=100,
        preserve=request.args.to_dict(flat=True),
        admin=admin_user,
        salario_minimo=cfg.salario_minimo or 0.0,
    )


@app.route("/admin/avaliacoes/export")
@admin_required
def admin_export_avaliacoes_csv():
    flash("Exportação em CSV ainda não foi implementada.", "warning")
    return redirect(url_for("admin_avaliacoes"))


@app.route("/filtrar_lancamentos")
@admin_required
def filtrar_lancamentos():
    qs = request.query_string.decode("utf-8")
    base = url_for("admin_lancamentos_split")
    joiner = "?" if qs else ""
    return redirect(f"{base}{joiner}{qs}")


@app.route("/exportar_lancamentos")
@admin_required
def exportar_lancamentos():
    args = request.args
    restaurante_id = args.get("restaurante_id", type=int)
    cooperado_id = args.get("cooperado_id", type=int)
    data_inicio = _parse_date(args.get("data_inicio"))
    data_fim = _parse_date(args.get("data_fim"))

    q = Lancamento.query

    if restaurante_id:
        q = q.filter(Lancamento.restaurante_id == restaurante_id)
    if cooperado_id:
        q = q.filter(Lancamento.cooperado_id == cooperado_id)
    if data_inicio:
        q = q.filter(Lancamento.data >= data_inicio)
    if data_fim:
        q = q.filter(Lancamento.data <= data_fim)

    lancs = q.order_by(Lancamento.data.desc(), Lancamento.id.desc()).all()

    wb = Workbook()
    ws = wb.active
    ws.title = "Lançamentos"

    bold = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center")
    header_fill = PatternFill("solid", fgColor="DDDDDD")
    currency_fmt = "#,##0.00"
    date_fmt = "DD/MM/YYYY"

    headers = [
        "Restaurante", "Periodo", "Cooperado", "Descricao",
        "Valor", "Data", "HoraInicio", "HoraFim",
        "INSS", "SEST", "Encargos", "Liquido",
    ]
    ws.append(headers)

    for i in range(1, len(headers) + 1):
        cell = ws.cell(row=1, column=i)
        cell.font = bold
        cell.alignment = center
        cell.fill = header_fill

    for l in lancs:
        v = float(l.valor or 0.0)
        inss = v * 0.04
        sest = v * 0.005
        encargos = inss + sest
        liq = v - encargos

        row = [
            l.restaurante.nome if getattr(l, "restaurante", None) else "",
            l.restaurante.periodo if getattr(l, "restaurante", None) else "",
            l.cooperado.nome if getattr(l, "cooperado", None) else "",
            l.descricao or "",
            v,
            l.data,
            _fmt_time(getattr(l, "hora_inicio", None)),
            _fmt_time(getattr(l, "hora_fim", None)),
            inss,
            sest,
            encargos,
            liq,
        ]
        ws.append(row)
        r = ws.max_row
        ws.cell(r, 5).number_format = currency_fmt
        ws.cell(r, 6).number_format = date_fmt
        ws.cell(r, 9).number_format = currency_fmt
        ws.cell(r, 10).number_format = currency_fmt
        ws.cell(r, 11).number_format = currency_fmt
        ws.cell(r, 12).number_format = currency_fmt

    for c in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(c)].width = 18

    mem = io.BytesIO()
    wb.save(mem)
    mem.seek(0)

    return send_file(
        mem,
        as_attachment=True,
        download_name="lancamentos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/admin/documentos")
@admin_required
def admin_documentos():
    documentos = Documento.query.order_by(Documento.enviado_em.desc()).all()
    return render_template("admin_documentos.html", documentos=documentos)


@app.post("/admin/documentos/upload")
@admin_required
def admin_upload_documento():
    f = request.form
    titulo = (f.get("titulo") or "").strip()
    categoria = (f.get("categoria") or "outro").strip()
    descricao = (f.get("descricao") or "").strip()
    arquivo = request.files.get("arquivo")

    if not titulo or not (arquivo and arquivo.filename):
        flash("Preencha o título e selecione o arquivo.", "warning")
        return redirect(url_for("admin_documentos"))

    nome_unico = salvar_documento_upload(arquivo)
    if not nome_unico:
        flash("Falha ao salvar o arquivo.", "danger")
        return redirect(url_for("admin_documentos"))

    d = Documento(
        titulo=titulo,
        categoria=categoria,
        descricao=descricao,
        arquivo_url=url_for("serve_documento", nome=nome_unico),
        arquivo_nome=nome_unico,
        enviado_em=datetime.utcnow(),
    )
    db.session.add(d)
    db.session.commit()
    flash("Documento enviado.", "success")
    return redirect(url_for("admin_documentos"))


@app.get("/admin/documentos/<int:doc_id>/delete")
@admin_required
def admin_delete_documento(doc_id):
    d = Documento.query.get_or_404(doc_id)
    try:
        p = resolve_documento_path(d.arquivo_nome)
        if p and os.path.exists(p):
            os.remove(p)
    except Exception:
        pass

    db.session.delete(d)
    db.session.commit()
    flash("Documento removido.", "success")
    return redirect(url_for("admin_documentos"))


@app.get("/docs/<path:nome>")
def serve_documento(nome: str):
    path = resolve_documento_path(nome)
    if not path:
        abort(404)

    mime, _ = mimetypes.guess_type(path)
    is_pdf = (mime == "application/pdf") or path.lower().endswith(".pdf")

    return send_file(
        path,
        mimetype=mime or "application/octet-stream",
        as_attachment=not is_pdf,
        download_name=os.path.basename(path),
        conditional=True,
    )


@app.route("/documentos")
def documentos_publicos():
    if not session.get("user_id"):
        return redirect(url_for("login"))
    documentos = Documento.query.order_by(Documento.enviado_em.desc()).all()
    return render_template("documentos_publicos.html", documentos=documentos)


@app.route("/documentos/<int:doc_id>/baixar")
def baixar_documento(doc_id):
    doc = Documento.query.get_or_404(doc_id)
    path = resolve_documento_path(doc.arquivo_nome)
    if not path or not os.path.exists(path):
        abort(404)
    return send_file(path, as_attachment=True, download_name=os.path.basename(doc.arquivo_nome))


@app.get("/admin/tabelas", endpoint="admin_tabelas")
@admin_required
def admin_tabelas():
    tabelas = Tabela.query.order_by(Tabela.enviado_em.desc(), Tabela.id.desc()).all()
    return render_template("admin_tabelas.html", tabelas=tabelas)


@app.post("/admin/tabelas/upload", endpoint="admin_upload_tabela")
@admin_required
def admin_upload_tabela():
    f = request.form
    titulo = (f.get("titulo") or "").strip()
    descricao = (f.get("descricao") or "").strip() or None
    arquivo = request.files.get("arquivo") or request.files.get("file") or request.files.get("tabela")

    if not titulo or not (arquivo and arquivo.filename):
        flash("Preencha o título e selecione o arquivo.", "warning")
        return redirect(url_for("admin_tabelas"))

    nome_salvo = salvar_tabela_upload(arquivo)
    if not nome_salvo:
        flash("Falha ao salvar a tabela.", "danger")
        return redirect(url_for("admin_tabelas"))

    t = Tabela(
        titulo=titulo,
        descricao=descricao,
        arquivo_url=nome_salvo,
        arquivo_nome=arquivo.filename,
        enviado_em=datetime.utcnow(),
    )
    db.session.add(t)
    db.session.commit()

    flash("Tabela publicada.", "success")
    return redirect(url_for("admin_tabelas"))


@app.get("/admin/tabelas/<int:tab_id>/delete", endpoint="admin_delete_tabela")
@admin_required
def admin_delete_tabela(tab_id: int):
    t = Tabela.query.get_or_404(tab_id)
    try:
        p = resolve_tabela_path(t.arquivo_url)
        if p and os.path.exists(p):
            os.remove(p)
    except Exception:
        pass

    db.session.delete(t)
    db.session.commit()
    flash("Tabela excluída.", "success")
    return redirect(url_for("admin_tabelas"))


@app.get("/tabelas/<int:tab_id>/abrir", endpoint="tabela_abrir")
def tabela_abrir(tab_id: int):
    if session.get("user_tipo") not in {"admin", "cooperado", "restaurante"}:
        return redirect(url_for("login"))

    t = Tabela.query.get_or_404(tab_id)
    p = resolve_tabela_path(t.arquivo_url)
    if not p:
        abort(404)

    return send_file(p, as_attachment=False, download_name=t.arquivo_nome or os.path.basename(p))


@app.get("/tabelas/<int:tab_id>/baixar", endpoint="baixar_tabela")
def baixar_tabela(tab_id: int):
    if session.get("user_tipo") not in {"admin", "cooperado", "restaurante"}:
        return redirect(url_for("login"))

    t = Tabela.query.get_or_404(tab_id)
    p = resolve_tabela_path(t.arquivo_url)
    if not p:
        abort(404)

    return send_file(p, as_attachment=True, download_name=t.arquivo_nome or os.path.basename(p))


@app.errorhandler(413)
def too_large(e):
    flash("Arquivo excede o tamanho máximo permitido (32MB).", "danger")
    return redirect(url_for("admin_documentos"))


try:
    if os.environ.get("INIT_DB_ON_START", "1") == "1":
        with app.app_context():
            init_db()
except Exception as e:
    try:
        app.logger.warning(f"init_db falhou: {e}")
    except Exception:
        pass


if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

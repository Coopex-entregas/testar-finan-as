
from __future__ import annotations

# ============ Stdlib ============
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
import os, io, csv, re, json, time, difflib, unicodedata
from datetime import datetime, date, timedelta, time as dtime
from collections import defaultdict, namedtuple
from functools import wraps
from types import SimpleNamespace

# MIME types (fixes p/ Office)
import mimetypes
mimetypes.add_type("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx")
mimetypes.add_type("application/vnd.ms-excel", ".xls")
mimetypes.add_type("application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx")
mimetypes.add_type("application/msword", ".doc")
mimetypes.add_type("application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx")
mimetypes.add_type("application/vnd.ms-powerpoint", ".ppt")

# ============ Terceiros ============
from flask import (
    Flask, render_template, request, redirect, url_for, session,
    flash, send_file, abort, jsonify, current_app
)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from dateutil.relativedelta import relativedelta

# ✅ FALTAVA ISSO (resolve o erro do UserMixin e já deixa pronto p/ login)
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    login_required,
    logout_user,
    current_user,
)

from sqlalchemy import text
from sqlalchemy import func, text as sa_text, or_, and_, case
from sqlalchemy import func, text as sa_text, or_, and_, case, literal
from sqlalchemy.inspection import inspect as sa_inspect
from sqlalchemy.pool import QueuePool
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError, IntegrityError, DisconnectionError
from sqlalchemy import delete as sa_delete

# 👉 Novo: para gerar XLSX em memória
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# ============ App / Diretórios ============
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

UPLOAD_DIR = os.path.join(BASE_DIR, "static", "uploads")
DOCS_DIR   = os.path.join(UPLOAD_DIR, "docs")
os.makedirs(DOCS_DIR, exist_ok=True)

# Persistência real (Render Disk)
PERSIST_ROOT = os.environ.get("PERSIST_ROOT", "/var/data")
if not os.path.isdir(PERSIST_ROOT):
    PERSIST_ROOT = os.path.join(BASE_DIR, "data")
os.makedirs(PERSIST_ROOT, exist_ok=True)

TABELAS_DIR = os.path.join(PERSIST_ROOT, "tabelas")
os.makedirs(TABELAS_DIR, exist_ok=True)

STATIC_TABLES = os.path.join(BASE_DIR, "static", "uploads", "tabelas")
os.makedirs(STATIC_TABLES, exist_ok=True)

# 🔹 Documentos (persistente em disco)
DOCS_PERSIST_DIR = os.path.join(PERSIST_ROOT, "docs")
os.makedirs(DOCS_PERSIST_DIR, exist_ok=True)



def _merge_qs(url: str, extra: dict[str, str]) -> str:
    """Insere parâmetros de query no URI sem duplicar os já existentes."""
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    for k, v in (extra or {}).items():
        q.setdefault(k, v)
    return urlunparse(p._replace(query=urlencode(q, doseq=True)))


def _build_db_uri() -> str:
    raw = os.environ.get("DATABASE_URL")
    if not raw:
        return "sqlite:///" + os.path.join(BASE_DIR, "app.db")

    # força driver psycopg3 (SQLAlchemy)
    if raw.startswith("postgres://"):
        raw = raw.replace("postgres://", "postgresql+psycopg://", 1)
    elif raw.startswith("postgresql://") and "+psycopg" not in raw:
        raw = raw.replace("postgresql://", "postgresql+psycopg://", 1)

    # SSL + keepalive + app name via libpq (idempotente)
    extras = {
        "sslmode": "require",
        "keepalives": "1",
        "keepalives_idle": "30",   # segundos ocioso antes de mandar keepalive
        "keepalives_interval": "10",
        "keepalives_count": "3",
        "application_name": os.environ.get("APP_NAME", "financas-dxsu"),
    }
    return _merge_qs(raw, extras)


app = Flask(__name__, static_folder="static", template_folder="templates")


# =========================
# Compat: Restaurante Avisos (evita 404 no front)
# =========================
@app.get("/api/rest/avisos/unread_count")
def api_rest_avisos_unread_count():
    """Endpoint de compatibilidade usado pelo painel do restaurante.
    Se você não usa módulo de avisos por restaurante, retorna 0.
    """
    try:
        return jsonify(count=0)
    except Exception:
        return jsonify(count=0)

app.secret_key = os.environ.get("SECRET_KEY", "coopex-secret")

URI = _build_db_uri()

# 🚫 Guard: impede cair em SQLite em produção se DATABASE_URL não existir
if "sqlite" in URI and os.environ.get("FLASK_ENV") == "production":
    raise RuntimeError("DATABASE_URL ausente em produção")

# ===== Pool dimensionado por worker =====
workers = int(os.environ.get("WEB_CONCURRENCY", "1") or "1")
threads = int(os.environ.get("GTHREADS", "1") or "1")  # se não usar threads, fica 1
req_concurrency = max(1, workers * threads)

# alvo global de conexões por instância (ajustável via env)
# 40 é bem suficiente para ~70 pessoas usando o sistema sem estourar limite do Postgres
target_total = int(os.environ.get("DB_TARGET_CONNS", "40") or "40")

# por worker, limitado para não exagerar:
per_worker_target = max(5, min(target_total // max(1, workers), 15))

default_pool_size = min(per_worker_target, req_concurrency + 2)
default_max_overflow = max(5, default_pool_size)

pool_size = int(os.environ.get("DB_POOL_SIZE", str(default_pool_size)))
max_overflow = int(os.environ.get("DB_MAX_OVERFLOW", str(default_max_overflow)))
pool_recycle = int(os.environ.get("SQL_POOL_RECYCLE", "240"))  # 4 min < timeout de idle do provider
pool_timeout = int(os.environ.get("SQL_POOL_TIMEOUT", "20"))   # espera máx. por conexão do pool (s)

app.config.update(
    SQLALCHEMY_DATABASE_URI=URI,
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    JSON_SORT_KEYS=False,
    MAX_CONTENT_LENGTH=32 * 1024 * 1024,  # 32MB
    SESSION_COOKIE_HTTPONLY=True,
    REMEMBER_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("FLASK_SECURE_COOKIES", "1") == "1",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
    SQLALCHEMY_ENGINE_OPTIONS={
        "poolclass": QueuePool,
        "pool_size": pool_size,
        "max_overflow": max_overflow,
        "pool_timeout": pool_timeout,
        "pool_pre_ping": True,       # testa conexão antes de usar (evita usar conn morta)
        "pool_use_lifo": True,       # reduz churn de conexões sob carga
        "pool_recycle": pool_recycle,  # recicla sockets ociosos (evita timeout de idle / TLS)
        "connect_args": {
            # tempo máx. para abrir conexão
            "connect_timeout": int(os.getenv("PGCONNECT_TIMEOUT", "5")),
            # tempo máx. por statement no servidor (defensivo)
            "options": "-c statement_timeout=15000",
        },
    },
)

db = SQLAlchemy(app)

def _sso_serializer():
    secret = os.environ.get("SSO_SHARED_SECRET") or app.secret_key
    # "salt" separa o token SSO de outros usos do secret
    return URLSafeTimedSerializer(secret_key=secret, salt="coopex-sso-v1")

def sso_load(token: str, max_age_seconds: int = 45) -> dict:
    s = _sso_serializer()
    return s.loads(token, max_age=max_age_seconds)

def _get_or_create_sso_user(tipo: str = "admin") -> Usuario:
    """
    Garante um usuário técnico para sessão SSO, evitando quebrar rotas que consultam Usuario.
    """
    username = f"sso_{tipo}"
    u = Usuario.query.filter_by(usuario=username).first()
    if u:
        return u

    # cria user técnico sem senha (não loga pelo /login)
    u = Usuario(usuario=username, tipo=tipo, senha_hash="!")
    db.session.add(u)
    db.session.commit()
    return u
    

def ajustar_banco():
    try:
        # se você tiver essa função _is_sqlite, use; senão pode checar pela URI
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(despesas_cooperado);")).fetchall()
            colnames = {row[1] for row in cols}
            if "eh_adiantamento" not in colnames:
                db.session.execute(sa_text(
                    "ALTER TABLE despesas_cooperado ADD COLUMN eh_adiantamento BOOLEAN DEFAULT 0"
                ))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.despesas_cooperado
                ADD COLUMN IF NOT EXISTS eh_adiantamento BOOLEAN DEFAULT FALSE
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()


# ========= Retry de conexão p/ rotas críticas =========
def with_db_retry(fn):
    """
    Decorator simples para repetir a operação 1x em caso de queda de conexão
    (OperationalError / DisconnectionError). Usa junto de rotas que batem no banco.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(2):  # tentativa 0 e 1
            try:
                return fn(*args, **kwargs)
            except (OperationalError, DisconnectionError) as e:
                last_exc = e
                # rollback defensivo e pequeno backoff
                db.session.rollback()
                try:
                    app.logger.warning(f"[DB_RETRY] tentativa {attempt+1} falhou: {e}")
                except Exception:
                    pass
                time.sleep(0.2)
        # se chegou aqui, estourou as tentativas
        raise last_exc
    return wrapper


# Health checks
@app.get("/healthz")
def healthz():
    return "ok", 200


@app.get("/readyz")
def readyz():
    try:
        db.session.execute(sa_text("SELECT 1"))
        return "ready", 200
    except Exception:
        return "not-ready", 503


# Liga foreign_keys no SQLite
@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_con, con_record):
    try:
        if app.config["SQLALCHEMY_DATABASE_URI"].startswith("sqlite"):
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



# ==========================================
# MODELOS
# ==========================================

class Usuario(db.Model, UserMixin):
    __tablename__ = "usuarios"

    id = db.Column(db.Integer, primary_key=True)
    usuario = db.Column(db.String(80), unique=True, nullable=False)
    senha_hash = db.Column(db.String(200), nullable=False)
    tipo = db.Column(db.String(20), nullable=False)  # admin | cooperado | restaurante

    # Importante: default no Python + default no BANCO
    ativo = db.Column(
        db.Boolean,
        nullable=False,
        default=True,
        server_default=text("true")  # Postgres; funciona bem no Render
    )

    @property
    def is_active(self) -> bool:
        # Flask-Login usa isso para bloquear login de usuário inativo
        return bool(self.ativo)

    def set_password(self, raw: str):
        self.senha_hash = generate_password_hash(raw)

    def check_password(self, raw: str) -> bool:
        return check_password_hash(self.senha_hash, raw)


class Cooperado(db.Model):
    __tablename__ = "cooperados"

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(120), nullable=False)

    usuario_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=False, unique=True)

    # 1 usuário -> 1 cooperado
    usuario_ref = db.relationship("Usuario", backref=db.backref("coop_account", uselist=False))

    # NOVO
    telefone = db.Column(db.String(30))

    # Foto no banco
    foto_bytes = db.Column(db.LargeBinary)
    foto_mime = db.Column(db.String(100))
    foto_filename = db.Column(db.String(255))
    foto_url = db.Column(db.String(255))

    cnh_numero = db.Column(db.String(50))
    cnh_validade = db.Column(db.Date)

    placa = db.Column(db.String(20))
    placa_validade = db.Column(db.Date)

    ultima_atualizacao = db.Column(db.DateTime)


class Restaurante(db.Model):
    __tablename__ = "restaurantes"

    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(120), nullable=False)
    periodo = db.Column(db.String(20), nullable=False)  # seg-dom | sab-sex | sex-qui

    usuario_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=False, unique=True)

    # 1 usuário -> 1 restaurante
    usuario_ref = db.relationship("Usuario", backref=db.backref("rest_account", uselist=False))

    # Foto no banco (bytea)
    foto_bytes = db.Column(db.LargeBinary)
    foto_mime = db.Column(db.String(100))
    foto_filename = db.Column(db.String(255))

    # compatibilidade
    foto_url = db.Column(db.String(255))


class Lancamento(db.Model):
    __tablename__ = "lancamentos"

    id = db.Column(db.Integer, primary_key=True)

    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=False)
    cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=False)

    restaurante = db.relationship("Restaurante")
    cooperado = db.relationship("Cooperado")

    descricao = db.Column(db.String(200))
    valor = db.Column(db.Float, default=0.0)

    # Se sempre precisa de data, deixe nullable=False
    data = db.Column(db.Date, nullable=False)

    hora_inicio = db.Column(db.String(10))
    hora_fim = db.Column(db.String(10))

    # opcional: quantidade de entregas
    qtd_entregas = db.Column(db.Integer)


# === AVALIAÇÕES DE COOPERADO (NOVO) =========================================
class AvaliacaoCooperado(db.Model):
    __tablename__ = "avaliacoes"
    id = db.Column(db.Integer, primary_key=True)

    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=False)
    cooperado_id   = db.Column(db.Integer, db.ForeignKey("cooperados.id"),  nullable=False)

    # 🔴 IMPORTANTE: CASCADE na FK para o lançamento
    lancamento_id  = db.Column(
        db.Integer,
        db.ForeignKey("lancamentos.id", ondelete="CASCADE"),
        nullable=True  # deixe True para permitir trocar para SET NULL futuramente, se quiser
    )

    # notas 1..5
    estrelas_geral         = db.Column(db.Integer)
    estrelas_pontualidade  = db.Column(db.Integer)
    estrelas_educacao      = db.Column(db.Integer)
    estrelas_eficiencia    = db.Column(db.Integer)
    estrelas_apresentacao  = db.Column(db.Integer)  # "Bem apresentado"

    comentario       = db.Column(db.Text)

    # IA/heurísticas
    media_ponderada  = db.Column(db.Float)
    sentimento       = db.Column(db.String(12))     # positivo | neutro | negativo
    temas            = db.Column(db.String(255))    # palavras-chave resumidas
    alerta_crise     = db.Column(db.Boolean, default=False)
    feedback_motoboy = db.Column(db.Text)

    criado_em = db.Column(db.DateTime, default=datetime.utcnow)


class ReceitaCooperativa(db.Model):
    __tablename__ = "receitas_coop"
    id = db.Column(db.Integer, primary_key=True)
    descricao = db.Column(db.String(200), nullable=False)
    valor_total = db.Column(db.Float, default=0.0)
    data = db.Column(db.Date, nullable=True)


class DespesaCooperativa(db.Model):
    __tablename__ = "despesas_coop"
    id = db.Column(db.Integer, primary_key=True)
    descricao = db.Column(db.String(200), nullable=False)
    valor = db.Column(db.Float, default=0.0)
    data = db.Column(db.Date)


class ReceitaCooperado(db.Model):
    __tablename__ = "receitas_cooperado"
    id = db.Column(db.Integer, primary_key=True)
    cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=False)
    cooperado = db.relationship("Cooperado")
    descricao = db.Column(db.String(200), nullable=False)
    valor = db.Column(db.Float, default=0.0)
    data = db.Column(db.Date)


# =========================
# Models
# =========================
class DespesaCooperado(db.Model):
    __tablename__ = "despesas_cooperado"
    id = db.Column(db.Integer, primary_key=True)
    cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=True)  # None = Todos
    cooperado = db.relationship("Cooperado")
    descricao = db.Column(db.String(200), nullable=False)
    valor = db.Column(db.Float, default=0.0)
    # legado (pontual)
    data = db.Column(db.Date)

    # novo (período)
    data_inicio = db.Column(db.Date)
    data_fim    = db.Column(db.Date)

    # NOVO: vínculo forte para sabermos quem gerou a despesa
    beneficio_id = db.Column(
        db.Integer,
        db.ForeignKey("beneficios_registro.id", ondelete="CASCADE"),
        index=True,
        nullable=True  # deixa True para migrar suave
    )

    # 🔴 NOVO: marca se é adiantamento
    eh_adiantamento = db.Column(db.Boolean, default=False)
    

class BeneficioRegistro(db.Model):
    __tablename__ = "beneficios_registro"
    id = db.Column(db.Integer, primary_key=True)
    data_inicial = db.Column(db.Date, nullable=False)
    data_final = db.Column(db.Date, nullable=False)
    data_lancamento = db.Column(db.Date)
    tipo = db.Column(db.String(40), nullable=False)  # hospitalar | farmaceutico | alimentar
    valor_total = db.Column(db.Float, default=0.0)
    recebedores_nomes = db.Column(db.Text)  # nomes separados por ';'
    recebedores_ids = db.Column(db.Text)    # ids separados por ';'

    # NOVO: relacionamento reverso (não remove nada seu)
    despesas = db.relationship(
        "DespesaCooperado",
        primaryjoin="BeneficioRegistro.id==DespesaCooperado.beneficio_id",
        cascade="all, delete-orphan",
        passive_deletes=True
    )

# =========================
# Semana seg→dom + Normalização automática
# =========================
from sqlalchemy import event

def semana_bounds(d: date):
    """
    Para uma data 'd', retorna (segunda, domingo) da mesma semana.
    0=segunda .. 6=domingo
    """
    dow = d.weekday()
    ini = d - timedelta(days=dow)     # segunda
    fim = ini + timedelta(days=6)     # domingo
    return ini, fim

def normaliza_periodo(data: date|None, data_inicio: date|None, data_fim: date|None):
    """
    Regra global para DespesaCooperado:
      - data_inicio = segunda
      - data_fim    = domingo
      - data        = domingo (== data_fim)
    Se vier só 'data', deriva o período pela semana dessa data.
    Se vier início/fim, ajusta para seg/dom da(s) semana(s) informada(s).
    """
    if data_inicio and data_fim:
        # garante ordem e aplica bordas semanais
        if data_fim < data_inicio:
            data_inicio, data_fim = data_fim, data_inicio
        ini, _ = semana_bounds(data_inicio)
        _, fim = semana_bounds(data_fim)
        return fim, ini, fim

    base = data or date.today()
    ini, fim = semana_bounds(base)
    return fim, ini, fim

# === Normalização automática de período em DespesaCooperado ===
def _ajusta_semana(target: DespesaCooperado):
    d, di, df = normaliza_periodo(target.data, target.data_inicio, target.data_fim)
    target.data = d
    target.data_inicio = di
    target.data_fim = df

@event.listens_for(DespesaCooperado, "before_insert")
def _desp_before_insert(mapper, connection, target):
    _ajusta_semana(target)

@event.listens_for(DespesaCooperado, "before_update")
def _desp_before_update(mapper, connection, target):
    _ajusta_semana(target)



class Escala(db.Model):
    __tablename__ = "escalas"
    id = db.Column(db.Integer, primary_key=True)
    cooperado_id = db.Column(
        db.Integer,
        db.ForeignKey("cooperados.id", ondelete="CASCADE"),
        nullable=True  # pode não ter cadastro
    )
    restaurante_id = db.Column(
        db.Integer,
        db.ForeignKey("restaurantes.id", ondelete="CASCADE"),
        nullable=True
    )

    data = db.Column(db.String(40))
    turno = db.Column(db.String(50))
    horario = db.Column(db.String(50))
    contrato = db.Column(db.String(80))
    cor = db.Column(db.String(200))
    cooperado_nome = db.Column(db.String(120))  # nome bruto da planilha quando não há cadastro


class TrocaSolicitacao(db.Model):
    __tablename__ = "trocas"
    id = db.Column(db.Integer, primary_key=True)
    solicitante_id = db.Column(
        db.Integer,
        db.ForeignKey("cooperados.id", ondelete="CASCADE"),
        nullable=False
    )
    destino_id = db.Column(
        db.Integer,
        db.ForeignKey("cooperados.id", ondelete="CASCADE"),
        nullable=False
    )
    origem_escala_id = db.Column(
        db.Integer,
        db.ForeignKey("escalas.id", ondelete="CASCADE"),
        nullable=False
    )
    mensagem = db.Column(db.Text)
    status = db.Column(db.String(20), default="pendente")
    criada_em = db.Column(db.DateTime, default=datetime.utcnow)
    aplicada_em = db.Column(db.DateTime)


class Config(db.Model):
    __tablename__ = "config"
    id = db.Column(db.Integer, primary_key=True)
    salario_minimo = db.Column(db.Float, default=0.0)


class Documento(db.Model):
    __tablename__ = "documentos"
    id = db.Column(db.Integer, primary_key=True)
    titulo = db.Column(db.String(200), nullable=False)
    categoria = db.Column(db.String(40))
    descricao = db.Column(db.String(255))
    arquivo_url = db.Column(db.String(255), nullable=False)
    arquivo_nome = db.Column(db.String(255))
    enviado_em = db.Column(db.DateTime, default=datetime.utcnow)


class Tabela(db.Model):

    id = db.Column(db.Integer, primary_key=True)
    titulo = db.Column(db.String(200), nullable=False)
    categoria = db.Column(db.String(40))
    descricao = db.Column(db.String(255))
    arquivo_url = db.Column(db.String(255), nullable=False)
    arquivo_nome = db.Column(db.String(255))
    enviado_em = db.Column(db.DateTime, default=datetime.utcnow)


# ---------- AVISOS (NOVO) ----------
aviso_restaurantes = db.Table(
    "aviso_restaurantes",
    db.Column("aviso_id", db.Integer, db.ForeignKey("avisos.id"), primary_key=True),
    db.Column("restaurante_id", db.Integer, db.ForeignKey("restaurantes.id"), primary_key=True),
)

class Aviso(db.Model):
    __tablename__ = "avisos"
    id = db.Column(db.Integer, primary_key=True)
    titulo = db.Column(db.String(140), nullable=False)
    corpo = db.Column(db.Text, nullable=False)
    # escopo: global | restaurante | cooperado
    tipo = db.Column(db.String(20), nullable=False, default="global")

    # destino individual (opcional)
    destino_cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"))
    destino_cooperado = db.relationship("Cooperado", foreign_keys=[destino_cooperado_id])

    # destino por restaurante (opcional, N:N)
    restaurantes = db.relationship("Restaurante", secondary=aviso_restaurantes, backref="avisos")

    prioridade = db.Column(db.String(10), default="normal")  # normal | alta
    fixado = db.Column(db.Boolean, default=False)
    ativo = db.Column(db.Boolean, default=True)
    inicio_em = db.Column(db.DateTime)  # janela de exibição opcional
    fim_em = db.Column(db.DateTime)

    criado_por_id = db.Column(db.Integer, db.ForeignKey("usuarios.id"), nullable=False)
    criado_em = db.Column(db.DateTime, default=datetime.utcnow)
    atualizado_em = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AvisoLeitura(db.Model):
    __tablename__ = "aviso_leituras"
    id = db.Column(db.Integer, primary_key=True)
    aviso_id = db.Column(db.Integer, db.ForeignKey("avisos.id"), nullable=False, index=True)
    cooperado_id = db.Column(db.Integer, db.ForeignKey("cooperados.id"), nullable=True, index=True)
    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=True, index=True)
    lido_em = db.Column(db.DateTime, default=datetime.utcnow)
    __table_args__ = (db.UniqueConstraint("aviso_id", "cooperado_id", "restaurante_id", name="uq_aviso_dest"), )

# =========================
# Helpers
# =========================
def _is_sqlite() -> bool:
    try:
        return db.session.get_bind().dialect.name == "sqlite"
    except Exception:
        return "sqlite" in (app.config.get("SQLALCHEMY_DATABASE_URI") or "")


from sqlalchemy.exc import InvalidRequestError

# Se não existir, define _is_sqlite() de forma segura
try:
    _is_sqlite  # type: ignore
except NameError:
    def _is_sqlite() -> bool:
        try:
            return db.engine.url.get_backend_name() == "sqlite"
        except Exception:
            return "sqlite" in str(db.engine.url).lower()

# ===== Alíquotas (parametrizáveis por env) =====
# Por padrão:
# INSS = 4% (0.04)
# SEST/SENAT = 0,5% (0.005)
INSS_ALIQ = float(os.environ.get("ALIQUOTA_INSS", "0.04"))
SEST_ALIQ = float(os.environ.get("ALIQUOTA_SEST", "0.005"))

def calc_descontos(valor: float) -> dict:
    """
    Calcula descontos padronizados (2 casas) e retorna um dicionário:
      - inss: 4%
      - sest: 0,5%
      - encargos: total (INSS + SEST)
      - liquido: valor - encargos
    """
    v = float(valor or 0.0)
    inss = round(v * INSS_ALIQ, 2)
    sest = round(v * SEST_ALIQ, 2)
    encargos = round(inss + sest, 2)
    liquido = round(v - encargos, 2)
    return {
        "inss": inss,
        "sest": sest,
        "encargos": encargos,
        "liquido": liquido,
    }


# =========================
# Init DB / Migração leve
# =========================
def init_db():
    """
    Versão unificada e idempotente:
      1) Ajustes de performance para SQLite (WAL/synchronous)
      2) Criação de todas as tabelas (create_all)
      3) Índices úteis (cooperado/restaurante/criado_em)
      4) Migrações leves (qtd_entregas, ativo em usuarios, colunas de escalas, fotos, tabela avaliacoes_restaurante)
      5) Bootstrap mínimo (admin e config) — só se os modelos existirem
    """

    # 1) Perf no SQLite
    try:
        if _is_sqlite():
            db.session.execute(sa_text("PRAGMA journal_mode=WAL;"))
            db.session.execute(sa_text("PRAGMA synchronous=NORMAL;"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 2) Tabelas/mapeamentos
    try:
        db.create_all()
    except Exception:
        db.session.rollback()

    # 3) Índices de performance (idempotentes)
    try:
        if _is_sqlite():
            stmts = [
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_criado_em   ON avaliacoes (criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_rest_criado ON avaliacoes (restaurante_id, criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_coop_criado ON avaliacoes (cooperado_id,  criado_em)",

                "CREATE INDEX IF NOT EXISTS ix_av_rest_criado_em      ON avaliacoes_restaurante (criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_av_rest_rest_criado    ON avaliacoes_restaurante (restaurante_id, criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_av_rest_coop_criado    ON avaliacoes_restaurante (cooperado_id,  criado_em)",
            ]
        else:
            stmts = [
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_criado_em   ON public.avaliacoes (criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_rest_criado ON public.avaliacoes (restaurante_id, criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_avaliacoes_coop_criado ON public.avaliacoes (cooperado_id,  criado_em)",

                "CREATE INDEX IF NOT EXISTS ix_av_rest_criado_em      ON public.avaliacoes_restaurante (criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_av_rest_rest_criado    ON public.avaliacoes_restaurante (restaurante_id, criado_em)",
                "CREATE INDEX IF NOT EXISTS ix_av_rest_coop_criado    ON public.avaliacoes_restaurante (cooperado_id,  criado_em)",
            ]

        for sql in stmts:
            db.session.execute(sa_text(sql))
        db.session.commit()
    except Exception:
        db.session.rollback()

    # 4) Migração leve: garantir coluna qtd_entregas em lancamentos
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(lancamentos);")).fetchall()
            colnames = {row[1] for row in cols}
            if "qtd_entregas" not in colnames:
                db.session.execute(sa_text("ALTER TABLE lancamentos ADD COLUMN qtd_entregas INTEGER"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF EXISTS public.lancamentos "
                "ADD COLUMN IF NOT EXISTS qtd_entregas INTEGER"
            ))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 4.x) período em despesas_cooperado (data_inicio / data_fim) + backfill
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(despesas_cooperado);")).fetchall()
            colnames = {row[1] for row in cols}

            if "data_inicio" not in colnames:
                db.session.execute(sa_text("ALTER TABLE despesas_cooperado ADD COLUMN data_inicio DATE"))
            if "data_fim" not in colnames:
                db.session.execute(sa_text("ALTER TABLE despesas_cooperado ADD COLUMN data_fim DATE"))
            db.session.commit()

            # Retropreenche linhas antigas
            db.session.execute(sa_text("""
                UPDATE despesas_cooperado
                   SET data_inicio = COALESCE(data_inicio, data),
                       data_fim    = COALESCE(data_fim,    data)
                 WHERE data IS NOT NULL
                   AND (data_inicio IS NULL OR data_fim IS NULL)
            """))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF NOT EXISTS public.despesas_cooperado
                ADD COLUMN IF NOT EXISTS data_inicio DATE
            """))
            db.session.execute(sa_text("""
                ALTER TABLE IF NOT EXISTS public.despesas_cooperado
                ADD COLUMN IF NOT EXISTS data_fim DATE
            """))
            db.session.commit()

            db.session.execute(sa_text("""
                UPDATE public.despesas_cooperado
                   SET data_inicio = COALESCE(data_inicio, data),
                       data_fim    = COALESCE(data_fim,    data)
                 WHERE data IS NOT NULL
                   AND (data_inicio IS NULL OR data_fim IS NULL)
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 4.y) beneficio_id em despesas_cooperado (FK p/ beneficios_registro)
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(despesas_cooperado);")).fetchall()
            colnames = {row[1] for row in cols}
            if "beneficio_id" not in colnames:
                db.session.execute(sa_text("ALTER TABLE despesas_cooperado ADD COLUMN beneficio_id INTEGER"))
            db.session.commit()
            # OBS: SQLite não permite adicionar uma FK com ON DELETE CASCADE via ALTER TABLE;
            # para ter a FK de fato, teria que recriar a tabela. Em dev, costuma bastar só a coluna.
        else:
            # Index para consultas
            db.session.execute(sa_text("""
                CREATE INDEX IF NOT EXISTS ix_despesas_beneficio_id
                ON public.despesas_cooperado (beneficio_id)
            """))
            # Normaliza sequência: DROP antes do ADD (idempotente entre deploys)
            db.session.execute(sa_text("""
                ALTER TABLE public.despesas_cooperado
                DROP CONSTRAINT IF EXISTS despesas_cooperado_beneficio_id_fkey
            """))
            db.session.execute(sa_text("""
                ALTER TABLE public.despesas_cooperado
                ADD CONSTRAINT despesas_cooperado_beneficio_id_fkey
                FOREIGN KEY (beneficio_id) REFERENCES public.beneficios_registro (id)
                ON DELETE CASCADE
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 4.z) eh_adiantamento em despesas_cooperado (marca adiantamento separado)
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(despesas_cooperado);")).fetchall()
            colnames = {row[1] for row in cols}
            if "eh_adiantamento" not in colnames:
                db.session.execute(sa_text("ALTER TABLE despesas_cooperado ADD COLUMN eh_adiantamento BOOLEAN DEFAULT 0"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                ALTER TABLE IF EXISTS public.despesas_cooperado
                ADD COLUMN IF NOT EXISTS eh_adiantamento BOOLEAN DEFAULT FALSE
            """))
            db.session.commit()
    except Exception:
        db.session.rollback()


    # 4.1) cooperado_nome em escalas
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(escalas);")).fetchall()
            colnames = {row[1] for row in cols}
            if "cooperado_nome" not in colnames:
                db.session.execute(sa_text("ALTER TABLE escalas ADD COLUMN cooperado_nome VARCHAR(120)"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS escalas "
                "ADD COLUMN IF NOT EXISTS cooperado_nome VARCHAR(120)"
            ))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 4.2) restaurante_id em escalas
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(escalas);")).fetchall()
            colnames = {row[1] for row in cols}
            if "restaurante_id" not in colnames:
                db.session.execute(sa_text("ALTER TABLE escalas ADD COLUMN restaurante_id INTEGER"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS escalas "
                "ADD COLUMN IF NOT EXISTS restaurante_id INTEGER"
            ))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 4.3) fotos no banco (cooperados)
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(cooperados);")).fetchall()
            colnames = {row[1] for row in cols}
            if "foto_bytes" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN foto_bytes BLOB"))
            if "foto_mime" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN foto_mime VARCHAR(100)"))
            if "foto_filename" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN foto_filename VARCHAR(255)"))
            if "foto_url" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN foto_url VARCHAR(255)"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_bytes BYTEA"))
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_mime VARCHAR(100)"))
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_filename VARCHAR(255)"))
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS cooperados ADD COLUMN IF NOT EXISTS foto_url VARCHAR(255)"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 4.3.x) telefone em cooperados
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(cooperados);")).fetchall()
            colnames = {row[1] for row in cols}
            if "telefone" not in colnames:
                db.session.execute(sa_text("ALTER TABLE cooperados ADD COLUMN telefone VARCHAR(30)"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS cooperados "
                "ADD COLUMN IF NOT EXISTS telefone VARCHAR(30)"
            ))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 4.4) tabela avaliacoes_restaurante (se não existir)
    try:
        if _is_sqlite():
            db.session.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS avaliacoes_restaurante (
                id SERIAL PRIMARY KEY,
                restaurante_id INTEGER NOT NULL,
                cooperado_id   INTEGER NOT NULL,
                lancamento_id  INTEGER UNIQUE,
                estrelas_geral INTEGER,
                estrelas_ambiente   INTEGER,
                estrelas_tratamento INTEGER,
                estrelas_suporte    INTEGER,
                comentario TEXT,
                media_ponderada DOUBLE PRECISION,
                sentimento VARCHAR(12),
                temas VARCHAR(255),
                alerta_crise BOOLEAN DEFAULT FALSE,
                criado_em TIMESTAMP
              );
          """))
            db.session.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS ix_av_rest_rest ON avaliacoes_restaurante(restaurante_id, criado_em)"))
            db.session.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS ix_av_rest_coop ON avaliacoes_restaurante(cooperado_id)"))
            db.session.commit()
        else:
            db.session.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS avaliacoes_restaurante (
                  id SERIAL PRIMARY KEY,
                  restaurante_id INTEGER NOT NULL,
                  cooperado_id   INTEGER NOT NULL,
                  lancamento_id  INTEGER UNIQUE,
                  estrelas_geral INTEGER,
                  estrelas_ambiente   = db.Column(db.Integer)
                  estrelas_tratamento = db.Column(db.Integer)
                  estrelas_suporte    = db.Column(db.Integer)
                  comentario TEXT,
                  media_ponderada DOUBLE PRECISION,
                  sentimento VARCHAR(12),
                  temas VARCHAR(255),
                  alerta_crise BOOLEAN DEFAULT FALSE,
                  criado_em TIMESTAMP
                );
            """))
            db.session.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS ix_av_rest_rest ON avaliacoes_restaurante(restaurante_id, criado_em)"))
            db.session.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS ix_av_rest_coop ON avaliacoes_restaurante(cooperado_id)"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 4.5) fotos no banco (restaurantes)
    try:
        if _is_sqlite():
            cols = db.session.execute(sa_text("PRAGMA table_info(restaurantes);")).fetchall()
            colnames = {row[1] for row in cols}
            if "foto_bytes" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN foto_bytes BLOB"))
            if "foto_mime" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN foto_mime VARCHAR(100)"))
            if "foto_filename" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN foto_filename VARCHAR(255)"))
            if "foto_url" not in colnames:
                db.session.execute(sa_text("ALTER TABLE restaurantes ADD COLUMN foto_url VARCHAR(255)"))
            db.session.commit()
        else:
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_bytes BYTEA"))
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_mime VARCHAR(100)"))
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_filename VARCHAR(255)"))
            db.session.execute(sa_text(
                "ALTER TABLE IF NOT EXISTS restaurantes ADD COLUMN IF NOT EXISTS foto_url VARCHAR(255)"))
            db.session.commit()
    except Exception:
        db.session.rollback()

    # 5) Bootstrap mínimo (admin e config) — só se os modelos existirem
    try:
        # Garante que o model Usuario está acessível
        _ = Usuario  # type: ignore[name-defined]

        # Admin
        try:
            tem_admin = Usuario.query.filter_by(tipo="admin").first()  # type: ignore[name-defined]
        except Exception:
            tem_admin = None

        if not tem_admin:
            admin_user = os.environ.get("ADMIN_USER", "admin")
            admin_pass = os.environ.get("ADMIN_PASS", os.urandom(8).hex())
            admin = Usuario(usuario=admin_user, tipo="admin", senha_hash="")  # type: ignore[name-defined]
            try:
                admin.set_password(admin_pass)  # type: ignore[attr-defined]
            except Exception:
                try:
                    from werkzeug.security import generate_password_hash
                    admin.senha_hash = generate_password_hash(admin_pass)  # type: ignore[attr-defined]
                except Exception:
                    pass
            db.session.add(admin)
            db.session.commit()
    except Exception:
        db.session.rollback()

    try:
        # Se o model Config existir, cria default
        try:
            Config  # type: ignore[name-defined]
            has_config_model = True
        except NameError:
            has_config_model = False

        if has_config_model:
            if not Config.query.get(1):  # type: ignore[name-defined]
                db.session.add(Config(id=1, salario_minimo=0.0))  # type: ignore[name-defined]
                db.session.commit()
    except Exception:
        db.session.rollback()

# === Bootstrap do banco no start (Render/Gunicorn) ===
try:
    if os.environ.get("INIT_DB_ON_START", "1") == "1":
        _t0 = datetime.utcnow()
        with app.app_context():
       
            init_db()
        try:
            app.logger.info(f"init_db concluído em {(datetime.utcnow() - _t0).total_seconds():.2f}s")
        except Exception:
            pass
    else:
        try:
            app.logger.info("INIT_DB_ON_START=0: pulando init_db no boot.")
        except Exception:
            pass
except Exception as e:
    try:
        app.logger.warning(f"init_db falhou/pulado: {e}")
    except Exception:
        pass


def get_config() -> Config:
    cfg = Config.query.get(1)
    if not cfg:
        cfg = Config(id=1, salario_minimo=0.0)
        db.session.add(cfg)
        db.session.commit()
    return cfg


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
    parts = [p.lower() for p in s.split() if p.strip()]
    return parts


def _norm_login(s: str) -> str:
    # remove acento, minúsculo e sem espaços
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower().strip()
    s = re.sub(r"\s+", "", s)
    return s

def _match_cooperado_by_login(login_planilha: str, cooperados: list[Cooperado]) -> Cooperado | None:
    """Casa EXATAMENTE com Usuario.usuario após normalização."""
    key = _norm_login(login_planilha)
    if not key:
        return None
    for c in cooperados:
        # c.usuario_ref.usuario é o login usado no sistema
        login = getattr(c.usuario_ref, "usuario", "") or ""
        if _norm_login(login) == key:
            return c
    return None


def _match_restaurante_id(contrato_txt: str) -> int | None:
    alvo = " ".join(_normalize_name(contrato_txt or ""))
    if not alvo:
        return None
    restaurantes = Restaurante.query.order_by(Restaurante.nome.asc()).all()

    for r in restaurantes:
        rn = " ".join(_normalize_name(r.nome))
        if alvo == rn or alvo in rn or rn in alvo:
            return r.id

    try:
        nomes_norm = [" ".join(_normalize_name(r.nome)) for r in restaurantes]
        close = difflib.get_close_matches(alvo, nomes_norm, n=1, cutoff=0.85)
        if close:
            alvo_norm = close[0]
            for r in restaurantes:
                if " ".join(_normalize_name(r.nome)) == alvo_norm:
                    return r.id
    except Exception:
        pass
    return None


def _match_cooperado_by_name(nome_planilha: str, cooperados: list[Cooperado]) -> Cooperado | None:
    def norm_join(s: str) -> str:
        return " ".join(_normalize_name(s))

    sheet_tokens = _normalize_name(nome_planilha)
    sheet_norm = " ".join(sheet_tokens)
    if not sheet_norm:
        return None

    for c in cooperados:
        c_norm = norm_join(c.nome)
        if sheet_norm == c_norm or sheet_norm in c_norm or c_norm in sheet_norm:
            return c

    parts_sheet = set(sheet_tokens)
    best, best_count = None, 0
    for c in cooperados:
        parts_c = set(_normalize_name(c.nome))
        inter = parts_sheet & parts_c
        if len(inter) > best_count:
            best, best_count = c, len(inter)
    if best and best_count >= 2:
        return best

    if len(sheet_tokens) == 1 and len(sheet_tokens[0]) >= 3:
        token = sheet_tokens[0]
        hits = [c for c in cooperados if token in set(_normalize_name(c.nome))]
        if os.environ.get("INIT_DB_ON_START", "0") == "1":
            return hits[0]

    names_norm = [norm_join(c.nome) for c in cooperados]
    close = difflib.get_close_matches(sheet_norm, names_norm, n=1, cutoff=0.85)
    if close:
        target = close[0]
        for c in cooperados:
            if norm_join(c.nome) == target:
                return c
    return None


def _build_docinfo(c: Cooperado) -> dict:
    today = date.today()
    cnh_ok = (c.cnh_validade is not None and c.cnh_validade >= today)
    placa_ok = (c.placa_validade is not None and c.placa_validade >= today)
    return {"cnh": {"ok": cnh_ok}, "placa": {"ok": placa_ok}}


def _save_upload(file_storage) -> str | None:
    # Mantido para compatibilidade com outras partes do app (ex.: uploads de xlsx)
    if not file_storage:
        return None
    fname = secure_filename(file_storage.filename or "")
    if not fname:
        return None
    path = os.path.join(UPLOAD_DIR, fname)
    file_storage.save(path)
    return f"/static/uploads/{fname}"

def salvar_tabela_upload(file_storage) -> str | None:
    """
    Salva o arquivo de TABELA dentro do diretório persistente (TABELAS_DIR)
    e retorna APENAS o nome do arquivo (para guardar no banco em Tabela.arquivo_nome).
    """
    if not file_storage or not file_storage.filename:
        return None
    fname = secure_filename(file_storage.filename)
    base, ext = os.path.splitext(fname)
    unique = f"{base}_{time.strftime('%Y%m%d_%H%M%S')}{ext.lower()}"
    destino = os.path.join(TABELAS_DIR, unique)
    file_storage.save(destino)
    return unique  # <- guarde este em Tabela.arquivo_nome


def resolve_tabela_path(nome_arquivo: str) -> str | None:
    """
    Resolve o caminho real de uma TABELA:
      1) /var/data/tabelas   (persistente)
      2) static/uploads/...  (legado)
    """
    if not nome_arquivo:
        return None
    candidatos = [
        os.path.join(TABELAS_DIR, nome_arquivo),
        os.path.join(STATIC_TABLES, nome_arquivo),  # legado
        # último fallback: se por acaso gravaram caminho completo em arquivo_url
        _abs_path_from_url(nome_arquivo) if nome_arquivo.startswith("/") else None,
    ]
    for p in candidatos:
        if p and os.path.isfile(p):
            return p
    # log amigável (vai parar com o WARNING que você viu)
    app.logger.warning("Arquivo de Tabela não encontrado. nome='%s' tents=%s",
                       nome_arquivo, [c for c in candidatos if c])
    return None

def _save_foto_to_db(entidade, file_storage, *, is_cooperado: bool) -> str | None:
    """
    Salva o arquivo enviado diretamente no banco (bytea/Blob) e
    retorna uma URL interna (/media/coop/<id> ou /media/rest/<id>).
    """
    if not file_storage or not file_storage.filename:
        return getattr(entidade, "foto_url", None)
    data = file_storage.read()
    if not data:
        return getattr(entidade, "foto_url", None)
    entidade.foto_bytes = data
    entidade.foto_mime = (file_storage.mimetype or "application/octet-stream")
    entidade.foto_filename = secure_filename(file_storage.filename)
    # garante que temos ID
    db.session.flush()
    if is_cooperado:
        url = url_for("media_coop", coop_id=entidade.id)
    else:
        url = url_for("media_rest", rest_id=entidade.id)
    entidade.foto_url = f"{url}?v={int(datetime.utcnow().timestamp())}"
    return entidade.foto_url

def _abs_path_from_url(rel_url: str) -> str:
    """
    Converte '/static/uploads/arquivo.pdf' para o caminho absoluto no disco.
    """
    if not rel_url:
        return ""
    # caminho padrão: /static/uploads/...
    if rel_url.startswith("/"):
        rel_url = rel_url.lstrip("/")
    return os.path.join(BASE_DIR, rel_url.replace("/", os.sep))

def _serve_uploaded(rel_url: str, *, download_name: str | None = None, force_download: bool = False):
    """
    Entrega um arquivo salvo em /static/uploads com mimetype correto.
    - PDFs abrem inline (no navegador) por padrão.
    - Se quiser forçar download, passe force_download=True.
    """
    if not rel_url:
        abort(404)
    abs_path = _abs_path_from_url(rel_url)
    if not os.path.exists(abs_path):
        abort(404)

    mime, _ = mimetypes.guess_type(abs_path)
    is_pdf = (mime == "application/pdf") or abs_path.lower().endswith(".pdf")
    return send_file(
        abs_path,
        mimetype=mime or "application/octet-stream",
        as_attachment=(force_download or not is_pdf),
        download_name=(download_name or os.path.basename(abs_path)),
        conditional=True,     # ajuda visualização/retomar download
    )

# ========= Helpers de DOCUMENTOS (PDFs, etc.) =========
def salvar_documento_upload(file_storage) -> str | None:
    """
    Salva o arquivo em disco persistente (/var/data/docs ou BASE_DIR/data/docs)
    e retorna APENAS o nome do arquivo (para guardar no banco em Documento.arquivo_nome).
    Requer que DOCS_PERSIST_DIR já exista (criado no Passo 1).
    """
    if not file_storage or not file_storage.filename:
        return None
    fname = secure_filename(file_storage.filename)
    base, ext = os.path.splitext(fname)
    unique = f"{base}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{ext.lower()}"
    destino = os.path.join(DOCS_PERSIST_DIR, unique)
    file_storage.save(destino)
    return unique  # <- gravar em Documento.arquivo_nome

def resolve_documento_path(nome_arquivo: str) -> str | None:
    """
    Resolve o caminho real do documento nesta ordem:
      1) persistente (/var/data/docs ou BASE_DIR/data/docs)
      2) legado (DOCS_DIR -> static/uploads/docs)
      3) caminho absoluto derivado de '/static/...'
    """
    if not nome_arquivo:
        return None
    candidatos = [
        os.path.join(DOCS_PERSIST_DIR, nome_arquivo),   # persistente
        os.path.join(DOCS_DIR, nome_arquivo),           # legado
        _abs_path_from_url(nome_arquivo) if str(nome_arquivo).startswith("/") else None,
    ]
    for p in candidatos:
        if p and os.path.isfile(p):
            return p
    app.logger.warning("Documento não encontrado. nome='%s' tents=%s",
                       nome_arquivo, [c for c in candidatos if c])
    return None

def _assert_cooperado_ativo(cooperado_id: int):
    c = (Cooperado.query
         .join(Usuario, Cooperado.usuario_id == Usuario.id)
         .filter(Cooperado.id == cooperado_id, Usuario.ativo.is_(True))
         .first())
    if not c:
        abort(400, description="Cooperado inativo ou inexistente.")
    return c

# ========= ROTA: /docs/<nome> (abre inline PDF; baixa outros tipos) =========
@app.get("/docs/<path:nome>")
def serve_documento(nome: str):
    """
    Abre PDFs inline (no navegador) e força download para outros tipos.
    Busca primeiro no disco persistente e faz fallback pro legado.
    """
    path = resolve_documento_path(nome)
    if not path:
        abort(404)

    mime, _ = mimetypes.guess_type(path)
    is_pdf = (mime == "application/pdf") or path.lower().endswith(".pdf")

    return send_file(
        path,
        mimetype=mime or "application/octet-stream",
        as_attachment=not is_pdf,                  # PDF inline, outros baixam
        download_name=os.path.basename(path),
        conditional=True,
    )

def _prox_ocorrencia_anual(dt: date | None) -> date | None:
    if not dt:
        return None
    hoje = date.today()
    alvo = date(hoje.year, dt.month, dt.day)
    if alvo < hoje:
        alvo = date(hoje.year + 1, dt.month, dt.day)
    return alvo

def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None
    

def _parse_ymd(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _bounds_mes(yyyy_mm: str):
    # "2025-06" -> [2025-06-01, 2025-07-01)
    y, m = map(int, yyyy_mm.split("-"))
    ini = date(y, m, 1)
    fim = (ini + relativedelta(months=1))
    return ini, fim
    

def _parse_data_ymd(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _fmt_br(d: date | None) -> str:
    return d.strftime("%d/%m/%Y") if d else ""


def _dow(dt: date) -> str:
    return str((dt.weekday() % 7) + 1)


# === Helpers de Avaliação (NLP leve + métricas) =============================
def _clamp_star(v):
    try:
        v = int(v)
    except Exception:
        return None
    return min(5, max(1, v))

def _media_ponderada(geral, pont, educ, efic, apres):
    """
    Ponderação (soma=1.0):
      Geral 0.40 + Pontualidade 0.15 + Educação 0.15 + Eficiência 0.15 + Bem Apresentado 0.15
    Calcula só com os campos presentes (ignora None) e renormaliza pesos.
    """
    pares = [
        (geral, 0.40),
        (pont,  0.15),
        (educ,  0.15),
        (efic,  0.15),
        (apres, 0.15),
    ]
    num = 0.0
    den = 0.0
    for nota, w in pares:
        if nota is not None:
            num += float(nota) * w
            den += w
    return round(num / den, 2) if den > 0 else None

_POS = set("""
bom ótima otimo excelente parabéns educado gentil atencioso cordial limpo cheiroso organizado rápido rapida rapido pontual
""".split())
_NEG = set("""
ruim péssimo pessimo horrível horrivel sujo atrasado grosseiro mal educado agressivo impaciente amassado quebrado frio derramou
""".split())

def _analise_sentimento(txt: str | None) -> str:
    if not txt:
        return "neutro"
    t = (txt or "").lower()
    # contagem bem simples
    pos = sum(1 for w in _POS if w in t)
    neg = sum(1 for w in _NEG if w in t)
    if neg > pos + 0: return "negativo"
    if pos > neg + 0: return "positivo"
    return "neutro"

# mapeia temas por palavras-chave simples
_TEMAS = {
    "Pontualidade":  ["pontual", "atras", "horario", "horário", "demor", "rápido", "rapido", "lent"],
    "Educação":      ["educad", "grosseir", "simpat", "antipatic", "mal trat", "sem paciencia", "sem paciência", "atencios"],
    "Eficiência":    ["amass", "vazou", "quebrad", "frio", "bagunça", "bagunca", "cuidado", "eficien", "desorgan"],
    "Bem apresentado": ["uniform", "higien", "apresenta", "limpo", "cheiroso", "aparencia", "aparência"],
}

def _identifica_temas(txt: str | None) -> list[str]:
    if not txt:
        return []
    t = (txt or "").lower()
    hits = []
    for tema, keys in _TEMAS.items():
        if any(k in t for k in keys):
            hits.append(tema)
    return hits[:4]

_RISCO = ["ameaça","ameaca","acidente","quebrado","agress","roubo","violên","violenc","lesão","lesao","sangue","caiu","bateu","droga","alcool","álcool"]

def _sinaliza_crise(nota_geral: int | None, txt: str | None) -> bool:
    if nota_geral == 1 and txt:
        low = txt.lower()
        return any(k in low for k in _RISCO)
    return False

def _gerar_feedback(pont, educ, efic, apres, comentario, sentimento):
    partes = []
    def badge(nome, nota):
        return f"{nome}: {nota} ★" if nota is not None else None

    for nome, nota in (("Pontualidade", pont), ("Educação", educ), ("Eficiência", efic), ("Apresentação", apres)):
        b = badge(nome, nota)
        if b: partes.append(b)

    dicas = []
    if educ is not None and educ <= 2: dicas.append("melhore a abordagem/educação ao falar com o cliente")
    if pont is not None and pont <= 2: dicas.append("tente chegar no horário combinado")
    if efic is not None and efic <= 2: dicas.append("redobre o cuidado com o pedido durante o transporte")
    if apres is not None and apres <= 2: dicas.append("capriche na apresentação pessoal (higiene/uniforme)")

    txt = f"Notas — " + " | ".join(partes) if partes else "Obrigado pelo trabalho!"
    if comentario:
        txt += f". Cliente comentou: \"{comentario.strip()}\""
    if dicas:
        txt += ". Dica: " + "; ".join(dicas) + "."
    if sentimento == "positivo":
        txt += " 👏"
    return txt[:1000]

from datetime import datetime, date
from sqlalchemy import or_, and_

# routes_avisos.py
from datetime import datetime
from flask import Blueprint, render_template, redirect, request, url_for, abort
from flask_login import current_user


def _cooperado_atual() -> Cooperado | None:
    """
    Retorna o Cooperado do usuário logado, usando a sessão da aplicação.
    """
    uid = session.get("user_id")
    if not uid:
        return None
    return Cooperado.query.filter_by(usuario_id=uid).first()

    # --- Blueprint Portal (topo do arquivo, depois de criar `app`) ---
portal_bp = Blueprint("portal", __name__, url_prefix="/portal")

@portal_bp.get("/avisos", endpoint="portal_cooperado_avisos")
@role_required("cooperado")
def avisos_list():
    coop = _cooperado_atual()
    if not coop:
        abort(403)

    # pega todos os avisos que se aplicam ao cooperado (seu helper)
    avisos = get_avisos_for_cooperado(coop)

    # busca leituras de uma vez (evita N+1)
    lidos_ids = {
        r.aviso_id
        for r in AvisoLeitura.query.filter_by(cooperado_id=coop.id).all()
    }

    # injeta flag lido para o template (sem tocar no banco)
    for a in avisos:
        a.lido = (a.id in lidos_ids)

    avisos_nao_lidos_count = sum(1 for a in avisos if not getattr(a, "lido", False))
    current_year = datetime.now().year

    return render_template(
        "portal_cooperado_avisos.html",
        avisos=avisos,
        avisos_nao_lidos_count=avisos_nao_lidos_count,
        current_year=current_year
    )

# === AVALIAÇÕES: Cooperado -> Restaurante (AJUSTADO) =========================
class AvaliacaoRestaurante(db.Model):
    __tablename__ = "avaliacoes_restaurante"

    id = db.Column(db.Integer, primary_key=True)

    restaurante_id = db.Column(db.Integer, db.ForeignKey("restaurantes.id"), nullable=False, index=True)
    cooperado_id   = db.Column(db.Integer, db.ForeignKey("cooperados.id"),   nullable=False, index=True)

    # 1 avaliação por lançamento (o cooperado só avalia uma vez aquele turno/expediente)
    lancamento_id  = db.Column(
        db.Integer,
        db.ForeignKey("lancamentos.id", ondelete="CASCADE"),
        unique=True,
        index=True,
        nullable=True,
    )

    # ===== SOMENTE 3 DIMENSÕES =====
    estrelas_ambiente   = db.Column(db.Integer)   # 1..5
    estrelas_tratamento = db.Column(db.Integer)   # 1..5
    estrelas_suporte    = db.Column(db.Integer)   # 1..5

    # Derivados
    estrelas_geral      = db.Column(db.Float)     # média arredondada 1 casa
    media_ponderada     = db.Column(db.Float)     # pode ser igual à geral (sem pesos)

    comentario          = db.Column(db.Text)
    sentimento          = db.Column(db.String(12))
    temas               = db.Column(db.String(255))
    alerta_crise        = db.Column(db.Boolean, default=False)

    criado_em = db.Column(db.DateTime, default=datetime.utcnow, index=True)

    def recompute(self, pesos=(1, 1, 1)):
        vals = [self.estrelas_ambiente, self.estrelas_tratamento, self.estrelas_suporte]
        nums = [float(v) for v in vals if isinstance(v, (int, float)) and v is not None]
        if not nums:
            self.estrelas_geral = None
            self.media_ponderada = None
            return
        if pesos and len(pesos) == 3:
            total = 0.0
            wsum = 0.0
            for v, p in zip(vals, pesos):
                if v is not None:
                    total += float(v) * p
                    wsum += p
            media = (total / wsum) if wsum > 0 else (sum(nums) / len(nums))
        else:
            media = sum(nums) / len(nums)
        self.media_ponderada = round(media, 2)
        self.estrelas_geral = round(media, 1)

@portal_bp.post("/avisos/<int:aviso_id>/lido", endpoint="marcar_aviso_lido")
@role_required("cooperado")
def avisos_marcar_lido(aviso_id: int):
    coop = _cooperado_atual()
    if not coop:
        abort(403)

    aviso = Aviso.query.get_or_404(aviso_id)

    # idempotente: só cria se ainda não houver leitura
    ja_leu = AvisoLeitura.query.filter_by(
        cooperado_id=coop.id,
        aviso_id=aviso.id
    ).first()

    if not ja_leu:
        db.session.add(AvisoLeitura(
            cooperado_id=coop.id,
            aviso_id=aviso.id,
            lido_em=datetime.utcnow()
        ))
        db.session.commit()

    next_url = request.form.get("next") or (url_for("portal.portal_cooperado_avisos") + f"#aviso-{aviso.id}")
    return redirect(next_url)

@portal_bp.post("/avisos/marcar-todos", endpoint="marcar_todos_avisos_lidos")
@role_required("cooperado")
def avisos_marcar_todos():
    coop = _cooperado_atual()
    if not coop:
        abort(403)

    avisos = get_avisos_for_cooperado(coop)
    if not avisos:
        return redirect(url_for("portal.portal_cooperado_avisos"))
    ids_todos = {a.id for a in avisos}
    ids_ja_lidos = {
        r.aviso_id
        for r in AvisoLeitura.query.filter_by(cooperado_id=coop.id).all()
    }
    ids_pendentes = list(ids_todos - ids_ja_lidos)

    if ids_pendentes:
        db.session.bulk_save_objects([
            AvisoLeitura(cooperado_id=coop.id, aviso_id=aid, lido_em=datetime.utcnow())
            for aid in ids_pendentes
        ])
        db.session.commit()

    return redirect(url_for("portal.portal_cooperado_avisos"))

  # --- Registro do blueprint 'portal' (uma única vez, após definir TODAS as rotas dele)
# --- Registro do blueprint 'portal' (depois de definir TODAS as rotas do blueprint)
def register_blueprints_once(app):
    if "portal" not in app.blueprints:
        app.register_blueprint(portal_bp)

register_blueprints_once(app)

# --- Alias para compatibilidade com o template (endpoint esperado: 'portal_cooperado_avisos')
from flask import redirect, url_for

def _portal_cooperado_avisos_alias():
    # redireciona para a rota real dentro do blueprint 'portal'
    return redirect(url_for("portal.portal_cooperado_avisos"))

# publica a URL "antiga" (ajuste o path se o seu antigo era outro)
app.add_url_rule(
    "/portal/cooperado/avisos",         # caminho acessado
    endpoint="portal_cooperado_avisos", # nome que o template usa no url_for(...)
    view_func=_portal_cooperado_avisos_alias,
    methods=["GET"],
)

# ======== Helpers p/ troca: data/weekday/turno ========
def _parse_data_escala_str(s: str) -> date | None:
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', str(s or ''))
    if not m:
        return None
    d_, mth, y = map(int, m.groups())
    if y < 100:
        y += 2000
    try:
        return date(y, mth, d_)
    except Exception:
        return None

def _weekday_from_data_str(s: str) -> int | None:
    dt = _parse_data_escala_str(s)
    if dt:
        return (dt.weekday() % 7) + 1
    txt = unicodedata.normalize("NFD", str(s or "").lower())
    txt = "".join(ch for ch in txt if unicodedata.category(ch) != "Mn")
    M = re.search(
        r"\b(seg|segunda|ter|terca|terça|qua|quarta|qui|quinta|sex|sexta|sab|sabado|sábado|dom|domingo)\b",
        txt
    )
    if not M:
        M = re.search(r"\b(seg|ter|qua|qui|sex|sab|dom)\b", txt)
        if not M:
            return None
    token = M.group(1)
    mapa = {
        "seg":1,"segunda":1,
        "ter":2,"terca":2,"terça":2,
        "qua":3,"quarta":3,
        "qui":4,"quinta":4,
        "sex":5,"sexta":5,
        "sab":6,"sabado":6,"sábado":6,
        "dom":7,"domingo":7,
    }
    return mapa.get(token)

def _weekday_abbr(num: int | None) -> str:
    return {1:"SEG",2:"TER",3:"QUA",4:"QUI",5:"SEX",6:"SÁB",7:"DOM"}.get(num or 0, "")

def _turno_bucket(turno: str | None, horario: str | None) -> str:
    t = (turno or "").lower()
    t = unicodedata.normalize("NFD", t)
    t = "".join(ch for ch in t if unicodedata.category(ch) != "Mn")
    if "noite" in t or "noturn" in t:
        return "noite"
    if any(x in t for x in ["dia", "diurn", "manha", "manhã", "tarde"]):
        return "dia"
    m = re.search(r'(\d{1,2}):(\d{2})', str(horario or ""))
    if m:
        h = int(m.group(1))
        return "noite" if (h >= 17 or h <= 6) else "dia"
    return "dia"

def _escala_label(e: Escala | None) -> str:
    if not e:
        return "—"
    wd = _weekday_from_data_str(e.data)
    wd_abbr = _weekday_abbr(wd)
    dt = _parse_data_escala_str(e.data)
    if dt:
        data_txt = dt.strftime("%d/%m/%y") + (f"-{wd_abbr}" if wd_abbr else "")
    else:
        data_txt = (str(e.data or "").strip() or wd_abbr)
    turno_txt = (e.turno or "").strip() or _turno_bucket(e.turno, e.horario).upper()
    horario_txt = (e.horario or "").strip()
    contrato_txt = (e.contrato or "").strip()
    parts = [x for x in [data_txt, turno_txt, horario_txt, contrato_txt] if x]
    return " • ".join(parts)

def _carry_forward_contrato(escalas: list[Escala]) -> dict[int, str]:
    eff = {}
    atual = ""
    for e in escalas:
        raw = (e.contrato or "").strip()
        if raw:
            atual = raw
        eff[e.id] = atual
    return eff

def _parse_linhas_from_msg(msg: str | None) -> list[dict]:
    if not msg:
        return []
    blobs = re.findall(r"__AFETACAO_JSON__\s*[:=]\s*(\{.*?\})\s*$", str(msg), flags=re.DOTALL)
    if not blobs:
        return []
    raw = blobs[-1]
    try:
        payload = json.loads(raw)
    except Exception:
        try:
            payload = json.loads(raw.replace("'", '\"'))
        except Exception:
            return []
    linhas = payload.get("linhas") or payload.get("rows") or []
    out = []
    for r in (linhas if isinstance(linhas, list) else []):
        turno = str(r.get("turno") or "").strip()
        horario = str(r.get("horario") or "").strip()
        turno_horario = (r.get("turno_horario") or " • ".join(x for x in [turno, horario] if x)).strip()
        out.append({
            "dia": str(r.get("dia") or ""),
            "turno_horario": turno_horario,
            "contrato": str(r.get("contrato") or ""),
            "saiu": str(r.get("saiu") or ""),
            "entrou": str(r.get("entrou") or ""),
        })
    return out

def _strip_afetacao_blob(msg: str | None) -> str:
    if not msg:
        return ""
    return re.sub(r"__AFETACAO_JSON__\s*[:=]\s*\{.*\}\s*$", "", str(msg), flags=re.DOTALL).strip()

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", str(s or "").strip().lower())
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s

def to_css_color(v: str) -> str:
    t = str(v or "").strip()
    if not t:
        return ""
    t_low = t.lower().strip()
    if re.fullmatch(r"[0-9a-fA-F]{8}", t):
        return f"#{t[2:8]}"
    if re.fullmatch(r"[0-9a-fA-F]{6}", t):
        return f"#{t}"
    if re.fullmatch(r"#?[0-9a-fA-F]{6,8}", t):
        if not t.startswith("#"):
            t = f"#{t}"
        if len(t) == 9:
            a = int(t[1:3], 16) / 255.0
            r = int(t[3:5], 16)
            g = int(t[5:7], 16)
            b = int(t[7:9], 16)
            return f"rgba({r},{g},{b},{a:.3f})"
        return t
    m = re.fullmatch(r"\s*(\d{1,3})\s*[,;]\s*(\d{1,3})\s*[,;]\s*(\d{1,3})\s*", t)
    if m:
        r, g, b = [max(0, min(255, int(x))) for x in m.groups()]
        return f"rgb({r},{g},{b})"
    mapa = {
        "azul": "blue", "vermelho": "red", "verde": "green",
        "amarelo": "yellow", "cinza": "gray", "preto": "black",
        "branco": "white", "laranja": "orange", "roxo": "purple",
    }
    return mapa.get(t_low, t)

# ---------- AVISOS: helpers ----------
from sqlalchemy import case, or_, and_, func
from sqlalchemy.orm import selectinload

def _avisos_base_query():
    # usa o relógio do banco; evita divergência de TZ/UTC da app
    now = func.now()
    return (
        Aviso.query
        .options(selectinload(Aviso.restaurantes))  # evita N+1 no template
        .filter(Aviso.ativo.is_(True))
        .filter(or_(Aviso.inicio_em.is_(None), Aviso.inicio_em <= now))
        .filter(or_(Aviso.fim_em.is_(None),    Aviso.fim_em    >= now))
    )

# PRIORIDADE: "alta" (0), "media"/"média" (1), outras/NULL (2)
_PRIORD = case(
    (func.lower(Aviso.prioridade) == "alta", 0),
    (func.lower(Aviso.prioridade).in_(("media", "média")), 1),
    else_=2,
)


def get_avisos_for_cooperado(coop: Cooperado):
    q = (
        _avisos_base_query()
        .filter(
            or_(
                (Aviso.tipo == "global"),
                and_(
                    Aviso.tipo == "cooperado",
                    or_(
                        Aviso.destino_cooperado_id == coop.id,
                        Aviso.destino_cooperado_id.is_(None)  # broadcast cooperados
                    ),
                ),
            )
        )
        .order_by(
            Aviso.fixado.desc(),
            _PRIORD.asc(),
            Aviso.criado_em.desc(),
        )
    )
    return q.all()

def get_avisos_for_restaurante(rest: Restaurante):
    """
    RESTAURANTE vê:
      - global
      - restaurante (broadcast ou destinado a ESTE restaurante)
    """
    q = (
        _avisos_base_query()
        .filter(
            or_(
                (Aviso.tipo == "global"),
                and_(
                    Aviso.tipo == "restaurante",
                    or_(
                        ~Aviso.restaurantes.any(),                  # broadcast
                        Aviso.restaurantes.any(Restaurante.id == rest.id),  # específico
                    ),
                ),
            )
        )
        .order_by(
            Aviso.fixado.desc(),
            _PRIORD.asc(),
            Aviso.criado_em.desc(),
        )
    )

    return q.all()

# =========================
# Rotas de mídia (fotos armazenadas no banco)
# =========================
from flask import Response

def _send_bytes_with_cache(data: bytes, mime: str, filename: str):
    """Envia bytes com Cache-Control/ETag para evitar hits repetidos no banco."""
    if not data:
        abort(404)
    rv = send_file(
        io.BytesIO(data),
        mimetype=(mime or "application/octet-stream"),
        as_attachment=False,
        download_name=filename,
        max_age=60 * 60 * 24 * 7,  # 7 dias
        conditional=True,          # habilita ETag/If-None-Match
        last_modified=None,
        etag=True,
    )
    # Cache explícito (defensivo)
    rv.headers["Cache-Control"] = "public, max-age=604800, immutable"
    return rv

@app.get("/media/coop/<int:coop_id>")
def media_coop(coop_id: int):
    # Retry simples para conexões quebradas (evita 500 pontual)
    try:
        c = Cooperado.query.get_or_404(coop_id)
    except OperationalError:
        db.session.rollback()
        c = Cooperado.query.get_or_404(coop_id)

    if c.foto_bytes:
        return _send_bytes_with_cache(
            c.foto_bytes,
            c.foto_mime or "image/jpeg",
            c.foto_filename or f"coop_{coop_id}.jpg",
        )
    # fallback para imagem padrão (não bate no banco novamente)
    return redirect(url_for("static", filename="img/default.png"))

@app.get("/media/rest/<int:rest_id>")
def media_rest(rest_id: int):
    try:
        r = Restaurante.query.get_or_404(rest_id)
    except OperationalError:
        db.session.rollback()
        r = Restaurante.query.get_or_404(rest_id)

    if r.foto_bytes:
        return _send_bytes_with_cache(
            r.foto_bytes,
            r.foto_mime or "image/jpeg",
            r.foto_filename or f"rest_{rest_id}.jpg",
        )
    return redirect(url_for("static", filename="img/default.png"))

# =========================
# Rota raiz
# =========================
@app.route("/")
def index():
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login"))
    u = Usuario.query.get(uid)
    if not u:
        return redirect(url_for("login"))
    if u.tipo == "admin":
        return redirect(url_for("admin_dashboard", tab="resumo"))
    if u.tipo == "cooperado":
        return redirect(url_for("portal_cooperado"))
    if u.tipo == "restaurante":
        return redirect(url_for("portal_restaurante"))
    return redirect(url_for("login"))

# =========================
# Auth
# =========================
@app.route("/login", methods=["GET", "POST"])
def login():
    erro_login = None

    if request.method == "POST":
        usuario = (request.form.get("usuario") or "").strip()
        senha   = request.form.get("senha") or ""

        u = Usuario.query.filter_by(usuario=usuario).first()

        # Fallback: permitir login usando o NOME do restaurante
        if not u:
            r = (Restaurante.query.filter(Restaurante.nome.ilike(usuario)).first()
                 or Restaurante.query.filter(Restaurante.nome.ilike(f"%{usuario}%")).first())
            if r and r.usuario_ref:
                u = r.usuario_ref

        if u and u.check_password(senha):
            # 🔴 BLOQUEIO: não permite login se a conta estiver inativa
            if not getattr(u, "ativo", True):
                flash("Conta desativada. Fale com o administrador.", "danger")
                return redirect(url_for("login"))

            # Autentica e direciona de acordo com o tipo
            session["user_id"] = u.id
            session["user_tipo"] = u.tipo
            if u.tipo == "admin":
                return redirect(url_for("admin_dashboard", tab="resumo"))
            elif u.tipo == "cooperado":
                return redirect(url_for("portal_cooperado"))
            elif u.tipo == "restaurante":
                return redirect(url_for("portal_restaurante"))

        erro_login = "Usuário/senha inválidos."
        flash(erro_login, "danger")

    # Renderização do formulário (template se existir; fallback simples se não)
    login_tpl = os.path.join("templates", "login.html")
    if os.path.exists(login_tpl):
        return render_template("login.html", erro_login=erro_login)

    return """
    <form method="POST" style="max-width:320px;margin:80px auto;font-family:Arial">
      <h3>Login</h3>
      <input name="usuario" placeholder="Usuário" style="width:100%;padding:10px;margin:6px 0">
      <input name="senha" type="password" placeholder="Senha" style="width:100%;padding:10px;margin:6px 0">
      <button style="padding:10px 16px">Entrar</button>
    </form>
    """


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.get("/sso/entrar")
def sso_entrar():
    token = (request.args.get("token") or "").strip()
    if not token:
        return redirect(url_for("login"))

    try:
        data = sso_load(token, max_age_seconds=45)
    except SignatureExpired:
        flash("Link expirou. Clique novamente no botão.", "danger")
        return redirect(url_for("login"))
    except BadSignature:
        flash("Link inválido.", "danger")
        return redirect(url_for("login"))

    # validações simples
    if data.get("aud") != "painel-destino":
        flash("Token com destino inválido.", "danger")
        return redirect(url_for("login"))

    # Só aceita SSO para admin/supervisao (ajuste conforme seu uso)
    tipo = (data.get("tipo") or "admin").strip().lower()
    if tipo not in ("admin", "supervisao"):
        tipo = "admin"

    u = _get_or_create_sso_user(tipo=tipo)

    session["user_id"] = u.id
    session["user_tipo"] = u.tipo

    # para onde entrar:
    # - se você criar um painel /supervisao, redirecione para ele
    # - caso contrário, use admin_dashboard (já existe no seu app) :contentReference[oaicite:1]{index=1}
    next_url = data.get("next") or url_for("admin_dashboard")
    return redirect(next_url)
    

# =========================
# Admin Dashboard
# =========================

from flask import jsonify, request, render_template, redirect, url_for, flash
from sqlalchemy import func, inspect
from sqlalchemy.exc import SQLAlchemyError, OperationalError, ProgrammingError
from datetime import date, timedelta
from collections import defaultdict, namedtuple
from types import SimpleNamespace
import re


@app.post("/admin/cooperados/<int:id>/toggle-status")
def toggle_status_cooperado(id):
    """
    Alterna o status 'ativo' do usuário vinculado ao cooperado.

    Observação crítica:
    - Se o campo/coluna 'ativo' ainda não existir no MODEL/DB, retorna erro orientando migração.
    """
    try:
        coop = db.session.get(Cooperado, id)
        if not coop or not getattr(coop, "usuario_ref", None):
            return jsonify(ok=False, error="Cooperado não encontrado"), 404

        user = coop.usuario_ref

        if not hasattr(user, "ativo"):
            return jsonify(
                ok=False,
                error="Campo 'ativo' ausente no modelo. Atualize o models.py (Usuario.ativo) e faça deploy."
            ), 500

        try:
            insp = inspect(db.engine)
            table = getattr(Usuario, "__tablename__", None)
            if not table:
                return jsonify(ok=False, error="Não foi possível identificar a tabela do modelo Usuario."), 500

            cols = {c["name"] for c in insp.get_columns(table)}
            if "ativo" not in cols:
                return jsonify(
                    ok=False,
                    error=(
                        "Coluna 'ativo' ausente no banco. Faça a migração/ALTER TABLE em produção "
                        f"(tabela: {table})."
                    ),
                    table=table
                ), 409
        except Exception:
            pass

        atual = bool(getattr(user, "ativo", True))
        user.ativo = (not atual)

        db.session.commit()
        return jsonify(ok=True, ativo=bool(user.ativo))

    except (OperationalError, ProgrammingError):
        db.session.rollback()
        return jsonify(
            ok=False,
            error="Falha ao salvar: provável falta da coluna 'ativo' no banco. Faça migração/ALTER TABLE."
        ), 409
    except SQLAlchemyError:
        db.session.rollback()
        return jsonify(ok=False, error="Falha ao salvar no banco"), 500


def _admin_dashboard_context(active_tab="resumo"):
    import time
    t0 = time.time()
    args = request.args

    considerar_periodo = False
    dows = set()

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
            dow_map = {
                1: 1,
                2: 2,
                3: 3,
                4: 4,
                5: 5,
                6: 6,
                7: 0,
            }

            db_dows = [dow_map[d] for d in dows_int if d in dow_map]

            if db_dows:
                q = q.filter(
                    db.extract("dow", Lancamento.data).in_(db_dows)
                )

    lanc_base = q.order_by(
        Lancamento.data.desc(),
        Lancamento.id.desc()
    ).all()

    lancamentos = lanc_base

    if considerar_periodo and restaurante_id:
        rest = Restaurante.query.get(restaurante_id)
        if rest:
            mapa = {
                "seg-dom": {"1", "2", "3", "4", "5", "6", "7"},
                "sab-sex": {"6", "7", "1", "2", "3", "4", "5"},
                "sex-qui": {"5", "6", "7", "1", "2", "3", "4"},
            }
            permitidos = mapa.get(rest.periodo, {"1", "2", "3", "4", "5", "6", "7"})
            lancamentos = [
                l for l in lancamentos
                if l.data and _dow(l.data) in permitidos
            ]

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

    receitas = rq.order_by(
        ReceitaCooperativa.data.desc().nullslast(),
        ReceitaCooperativa.id.desc()
    ).all()

    despesas = dq.order_by(
        DespesaCooperativa.data.desc(),
        DespesaCooperativa.id.desc()
    ).all()

    total_receitas = sum((r.valor_total or 0.0) for r in receitas)
    total_despesas = sum((d.valor or 0.0) for d in despesas)

    rq2 = ReceitaCooperado.query
    dq2 = DespesaCooperado.query

    if data_inicio:
        rq2 = rq2.filter(ReceitaCooperado.data >= data_inicio)

    if data_fim:
        rq2 = rq2.filter(ReceitaCooperado.data <= data_fim)

    if data_inicio and data_fim:
        dq2 = dq2.filter(
            DespesaCooperado.data_inicio <= data_fim,
            DespesaCooperado.data_fim >= data_inicio,
        )
    elif data_inicio:
        dq2 = dq2.filter(DespesaCooperado.data_fim >= data_inicio)
    elif data_fim:
        dq2 = dq2.filter(DespesaCooperado.data_inicio <= data_fim)

    receitas_coop = rq2.order_by(
        ReceitaCooperado.data.desc(),
        ReceitaCooperado.id.desc()
    ).all()

    despesas_coop = dq2.order_by(
        DespesaCooperado.data_fim.desc().nullslast(),
        DespesaCooperado.id.desc()
    ).all()

    total_receitas_coop = sum((r.valor or 0.0) for r in receitas_coop)

    total_despesas_coop = sum(
        (d.valor or 0.0)
        for d in despesas_coop
        if not d.eh_adiantamento
    )

    total_adiantamentos_coop = sum(
        (d.valor or 0.0)
        for d in despesas_coop
        if d.eh_adiantamento
    )

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

    folha_por_coop = []
    folha_inicio = None
    folha_fim = None

    if active_tab == "folha":
        folha_inicio = _parse_date(args.get("folha_inicio")) or (date.today() - timedelta(days=30))
        folha_fim = _parse_date(args.get("folha_fim")) or date.today()

        INSS_ALIQ_FOLHA = 0.04
        SEST_ALIQ_FOLHA = 0.005

        FolhaItem = namedtuple(
            "FolhaItem",
            "cooperado lancamentos receitas despesas bruto inss sest encargos outras_desp liquido"
        )

        for c in cooperados:
            l = (
                Lancamento.query.filter(
                    Lancamento.cooperado_id == c.id,
                    Lancamento.data >= folha_inicio,
                    Lancamento.data <= folha_fim,
                )
                .order_by(Lancamento.data.asc(), Lancamento.id.asc())
                .all()
            )
            r = (
                ReceitaCooperado.query.filter(
                    ReceitaCooperado.cooperado_id == c.id,
                    ReceitaCooperado.data >= folha_inicio,
                    ReceitaCooperado.data <= folha_fim,
                )
                .order_by(ReceitaCooperado.data.asc(), ReceitaCooperado.id.asc())
                .all()
            )
            d = (
                DespesaCooperado.query.filter(
                    (DespesaCooperado.cooperado_id == c.id) | (DespesaCooperado.cooperado_id.is_(None)),
                    DespesaCooperado.data_inicio <= folha_fim,
                    DespesaCooperado.data_fim >= folha_inicio,
                )
                .order_by(DespesaCooperado.data_inicio.asc(), DespesaCooperado.id.asc())
                .all()
            )

            bruto_lanc = sum((x.valor or 0) for x in l)

            inss = round(bruto_lanc * INSS_ALIQ_FOLHA, 2)
            sest = round(bruto_lanc * SEST_ALIQ_FOLHA, 2)
            encargos = round(inss + sest, 2)

            outras_desp = sum((x.valor or 0) for x in d)
            bruto_total = bruto_lanc + sum((x.valor or 0) for x in r)
            liquido = round(bruto_total - encargos - outras_desp, 2)

            for x in l:
                x.conta_inss = True
                x.isento_benef = False
                x.inss = round((x.valor or 0) * INSS_ALIQ_FOLHA, 2)
                x.sest = round((x.valor or 0) * SEST_ALIQ_FOLHA, 2)
                x.encargos = round((x.inss or 0) + (x.sest or 0), 2)

            folha_por_coop.append(
                FolhaItem(
                    cooperado=c,
                    lancamentos=l,
                    receitas=r,
                    despesas=d,
                    bruto=round(bruto_total, 2),
                    inss=inss,
                    sest=sest,
                    encargos=encargos,
                    outras_desp=round(outras_desp, 2),
                    liquido=liquido,
                )
            )

    def _tokenize(s: str):
        return [x.strip() for x in re.split(r"[;,]", s or "") if x.strip()]

    def _d(s):
        if not s:
            return None
        s = s.strip()
        try:
            if "/" in s:
                d_, m_, y_ = s.split("/")
                return date(int(y_), int(m_), int(d_))
            y_, m_, d_ = s.split("-")
            return date(int(y_), int(m_), int(d_))
        except Exception:
            return None

    b_ini = _d(request.args.get("b_ini"))
    b_fim = _d(request.args.get("b_fim"))
    coop_filter = request.args.get("coop_benef_id", type=int)

    q_benef = BeneficioRegistro.query

    if b_ini and b_fim:
        q_benef = q_benef.filter(
            BeneficioRegistro.data_inicial <= b_fim,
            BeneficioRegistro.data_final >= b_ini,
        )
    elif b_ini:
        q_benef = q_benef.filter(BeneficioRegistro.data_final >= b_ini)
    elif b_fim:
        q_benef = q_benef.filter(BeneficioRegistro.data_inicial <= b_fim)

    historico_beneficios = q_benef.order_by(BeneficioRegistro.id.desc()).all()

    beneficios_view = []
    for b in historico_beneficios:
        nomes = _tokenize(b.recebedores_nomes or "")
        ids = _tokenize(b.recebedores_ids or "")

        recs = []
        for i, nome in enumerate(nomes):
            rid = None
            if i < len(ids) and str(ids[i]).isdigit():
                try:
                    rid = int(ids[i])
                except Exception:
                    rid = None

            if coop_filter and (rid is not None) and (rid != coop_filter):
                continue

            recs.append({"id": rid, "nome": nome})

        if coop_filter and not recs:
            continue

        beneficios_view.append({
            "id": b.id,
            "data_inicial": b.data_inicial,
            "data_final": b.data_final,
            "data_lancamento": b.data_lancamento,
            "tipo": b.tipo,
            "valor_total": b.valor_total or 0.0,
            "recebedores": recs,
        })

    def _escala_desc(e):
        return _escala_label(e) if e else ""

    def _split_turno_horario(s: str) -> tuple[str, str]:
        if not s:
            return "", ""
        parts = [p.strip() for p in s.split("•")]
        if len(parts) == 2:
            return parts[0], parts[1]
        return s.strip(), ""

    def _linha_from_escala(e: Escala, saiu: str, entrou: str) -> dict:
        return {
            "dia": _escala_label(e).split(" • ")[0],
            "turno_horario": " • ".join(
                [x for x in [(e.turno or "").strip(), (e.horario or "").strip()] if x]
            ),
            "contrato": (e.contrato or "").strip(),
            "saiu": saiu,
            "entrou": entrou,
        }

    trocas_all = TrocaSolicitacao.query.order_by(TrocaSolicitacao.id.desc()).all()
    trocas_pendentes = []
    trocas_historico = []
    trocas_historico_flat = []

    for t in trocas_all:
        solicitante = Cooperado.query.get(t.solicitante_id)
        destinatario = Cooperado.query.get(t.destino_id)
        orig = Escala.query.get(t.origem_escala_id)

        linhas_afetadas = _parse_linhas_from_msg(t.mensagem) if t.status == "aprovada" else []

        if t.status == "aprovada" and not linhas_afetadas and orig and solicitante and destinatario:
            linhas_afetadas.append(_linha_from_escala(orig, saiu=solicitante.nome, entrou=destinatario.nome))

            wd_o = _weekday_from_data_str(orig.data)
            buck_o = _turno_bucket(orig.turno, orig.horario)
            candidatas = Escala.query.filter_by(cooperado_id=destinatario.id).all()
            best = None
            for e in candidatas:
                if _weekday_from_data_str(e.data) == wd_o and _turno_bucket(e.turno, e.horario) == buck_o:
                    if (orig.contrato or "").strip().lower() == (e.contrato or "").strip().lower():
                        best = e
                        break
                    if best is None:
                        best = e
            if best:
                linhas_afetadas.append(_linha_from_escala(best, saiu=destinatario.nome, entrou=solicitante.nome))

        destino_data = ""
        destino_turno = ""
        destino_horario = ""
        destino_contrato = ""

        if t.status == "aprovada" and linhas_afetadas and solicitante and destinatario:
            linha_dest = None
            for r_ in linhas_afetadas:
                if r_.get("saiu") == destinatario.nome and r_.get("entrou") == solicitante.nome:
                    linha_dest = r_
                    break
            if linha_dest:
                destino_data = linha_dest.get("dia", "")
                turno_txt, horario_txt = _split_turno_horario(linha_dest.get("turno_horario", ""))
                destino_turno = turno_txt
                destino_horario = horario_txt
                destino_contrato = linha_dest.get("contrato", "")

        if not destino_data and orig and destinatario:
            wd_o = _weekday_from_data_str(orig.data)
            buck_o = _turno_bucket(orig.turno, orig.horario)
            candidatas = Escala.query.filter_by(cooperado_id=destinatario.id).all()
            best = None
            for e in candidatas:
                if _weekday_from_data_str(e.data) == wd_o and _turno_bucket(e.turno, e.horario) == buck_o:
                    if (orig.contrato or "").strip().lower() == (e.contrato or "").strip().lower():
                        best = e
                        break
                    if best is None:
                        best = e
            if best:
                destino_data = best.data
                destino_turno = (best.turno or "").strip()
                destino_horario = (best.horario or "").strip()
                destino_contrato = (best.contrato or "").strip()

        item = {
            "id": t.id,
            "status": t.status,
            "mensagem": t.mensagem,
            "criada_em": t.criada_em,
            "aplicada_em": t.aplicada_em,
            "solicitante": solicitante,
            "destinatario": destinatario,
            "origem": orig,
            "destino": destinatario,
            "origem_desc": _escala_desc(orig),
            "origem_weekday": _weekday_from_data_str(orig.data) if orig else None,
            "origem_turno_bucket": _turno_bucket(orig.turno if orig else None, orig.horario if orig else None),
            "destino_data": destino_data,
            "destino_turno": destino_turno,
            "destino_horario": destino_horario,
            "destino_contrato": destino_contrato,
            "linhas_afetadas": linhas_afetadas,
        }

        if t.status == "aprovada" and linhas_afetadas:
            itens = []
            for r_ in linhas_afetadas:
                turno_txt, horario_txt = _split_turno_horario(r_.get("turno_horario", ""))
                itens.append(
                    {
                        "data": r_.get("dia", ""),
                        "turno": turno_txt,
                        "horario": horario_txt,
                        "contrato": r_.get("contrato", ""),
                        "saiu_nome": r_.get("saiu", ""),
                        "entrou_nome": r_.get("entrou", ""),
                    }
                )
                trocas_historico_flat.append(
                    {
                        "data": r_.get("dia", ""),
                        "turno": turno_txt,
                        "horario": horario_txt,
                        "contrato": r_.get("contrato", ""),
                        "saiu_nome": r_.get("saiu", ""),
                        "entrou_nome": r_.get("entrou", ""),
                        "aplicada_em": t.aplicada_em,
                    }
                )
            item["itens"] = itens

        (trocas_pendentes if t.status == "pendente" else trocas_historico).append(item)

    current_date = date.today()
    data_limite = date(current_date.year, 12, 31)

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
        "beneficios_view": beneficios_view,
        "historico_beneficios": historico_beneficios,
        "current_date": current_date,
        "data_limite": data_limite,
        "admin": admin_user,
        "docinfo_map": docinfo_map,
        "escalas_por_coop": esc_by_int,
        "escalas_por_coop_json": esc_by_str,
        "qtd_escalas_map": qtd_escalas_map,
        "qtd_escalas_sem_cadastro": qtd_sem_cadastro,
        "status_doc_por_coop": status_doc_por_coop,
        "chart_data_lancamentos_coop": chart_data_lancamentos_coop,
        "chart_data_lancamentos_cooperados": chart_data_lancamentos_cooperados,
        "folha_inicio": folha_inicio,
        "folha_fim": folha_fim,
        "folha_por_coop": folha_por_coop,
        "trocas_pendentes": trocas_pendentes,
        "trocas_historico": trocas_historico,
        "trocas_historico_flat": trocas_historico_flat,
    }


# Rotas split do painel admin
@app.route("/admin", methods=["GET"])
@admin_required
def admin_dashboard():
    return redirect(url_for("admin_resumo_split"))

@app.get("/admin/resumo")
@admin_required
def admin_resumo_split():
    ctx = _admin_dashboard_context("resumo")
    return render_template("resumo.html", **ctx)

@app.get("/admin/lancamentos")
@admin_required
def admin_lancamentos_split():
    ctx = _admin_dashboard_context("lancamentos")
    return render_template("lancamentos.html", **ctx)

@app.get("/admin/receitas")
@admin_required
def admin_receitas_split():
    ctx = _admin_dashboard_context("receitas")
    return render_template("receitas.html", **ctx)

@app.get("/admin/despesas")
@admin_required
def admin_despesas_split():
    ctx = _admin_dashboard_context("despesas")
    return render_template("despesas.html", **ctx)

@app.get("/admin/coop_receitas")
@admin_required
def admin_coop_receitas_split():
    ctx = _admin_dashboard_context("coop_receitas")
    return render_template("coop_receitas.html", **ctx)

@app.get("/admin/coop_despesas")
@admin_required
def admin_coop_despesas_split():
    ctx = _admin_dashboard_context("coop_despesas")
    return render_template("coop_despesas.html", **ctx)

@app.get("/admin/beneficios")
@admin_required
def admin_beneficios_split():
    ctx = _admin_dashboard_context("beneficios")
    return render_template("beneficios.html", **ctx)

@app.get("/admin/cooperados")
@admin_required
def admin_cooperados_split():
    ctx = _admin_dashboard_context("cooperados")
    return render_template("cooperados.html", **ctx)

@app.get("/admin/restaurantes")
@admin_required
def admin_restaurantes_split():
    ctx = _admin_dashboard_context("restaurantes")
    return render_template("restaurantes.html", **ctx)

@app.get("/admin/escalas")
@admin_required
def admin_escalas_split():
    ctx = _admin_dashboard_context("escalas")
    return render_template("escalas.html", **ctx)

@app.get("/admin/config")
@admin_required
def admin_config_split():
    ctx = _admin_dashboard_context("config")
    return render_template("config.html", **ctx)

@app.route("/admin/avisos", methods=["GET", "POST"], endpoint="admin_avisos")
@admin_required
def admin_avisos():
    avisos = Aviso.query.order_by(Aviso.fixado.desc(), Aviso.criado_em.desc()).all()
    cooperados = Cooperado.query.order_by(Cooperado.nome.asc()).all()
    restaurantes = Restaurante.query.order_by(Restaurante.nome.asc()).all()
    return render_template("admin_avisos.html", avisos=avisos, cooperados=cooperados, restaurantes=restaurantes)

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

# =========================================================
# ROTAS DE AÇÃO DO PAINEL ADMIN - ADAPTADAS PARA O SPLIT
# =========================================================

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
    admin.usuario = request.form.get("usuario", admin.usuario).strip()
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

    c = Cooperado(
        nome=nome,
        usuario_id=u.id,
        telefone=telefone,
        ultima_atualizacao=datetime.now(),
    )
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
        escala_ids = [eid for (eid,) in db.session.query(Escala.id)
                      .filter(Escala.cooperado_id == id).all()]

        if escala_ids:
            db.session.execute(
                sa_delete(TrocaSolicitacao)
                .where(TrocaSolicitacao.origem_escala_id.in_(escala_ids))
            )
            db.session.execute(
                sa_delete(Escala)
                .where(Escala.id.in_(escala_ids))
            )

        db.session.execute(
            sa_delete(TrocaSolicitacao)
            .where(or_(
                TrocaSolicitacao.solicitante_id == id,
                TrocaSolicitacao.destino_id == id
            ))
        )

        db.session.execute(
            sa_delete(AvaliacaoCooperado).where(AvaliacaoCooperado.cooperado_id == id)
        )
        db.session.execute(
            sa_delete(AvaliacaoRestaurante).where(AvaliacaoRestaurante.cooperado_id == id)
        )

        db.session.execute(
            sa_delete(Lancamento).where(Lancamento.cooperado_id == id)
        )

        db.session.execute(
            sa_delete(ReceitaCooperado).where(ReceitaCooperado.cooperado_id == id)
        )
        db.session.execute(
            sa_delete(DespesaCooperado).where(DespesaCooperado.cooperado_id == id)
        )

        db.session.execute(
            sa_delete(AvisoLeitura).where(AvisoLeitura.cooperado_id == id)
        )

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
    nome = f.get("nome", "").strip()
    periodo = f.get("periodo", "seg-dom")
    usuario_login = f.get("usuario", "").strip()
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

    r.nome = f.get("nome", "").strip()
    r.periodo = f.get("periodo", "seg-dom")
    r.usuario_ref.usuario = f.get("usuario", "").strip()

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
        escala_ids = [e.id for e in Escala.query.with_entities(Escala.id)
                      .filter(Escala.restaurante_id == id).all()]

        if escala_ids:
            db.session.execute(
                sa_delete(TrocaSolicitacao)
                .where(TrocaSolicitacao.origem_escala_id.in_(escala_ids))
            )
            db.session.execute(
                sa_delete(Escala)
                .where(Escala.restaurante_id == id)
            )

        db.session.execute(
            sa_delete(Lancamento).where(Lancamento.restaurante_id == id)
        )

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
            "ok": (cooperado.cnh_validade is not None and cooperado.cnh_validade >= hoje),
            "modo": "auto",
        },
        "placa": {
            "numero": cooperado.placa,
            "validade": cooperado.placa_validade,
            "prox_validade": _prox_ocorrencia_anual(cooperado.placa_validade),
            "ok": (cooperado.placa_validade is not None and cooperado.placa_validade >= hoje),
            "modo": "auto",
        }
    }

    return render_template(
        "editar_tabelas.html",
        cooperado=cooperado,
        coop=cooperado,
        c=cooperado,
        docinfo=docinfo
    )

# =========================
# Navegação/Export util
# =========================
@app.route("/filtrar_lancamentos")
@admin_required
def filtrar_lancamentos():
    qs = request.query_string.decode("utf-8")
    base = url_for("admin_lancamentos_split")
    joiner = "?" if qs else ""
    return redirect(f"{base}{joiner}{qs}")


from datetime import datetime, date

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

def _dow(d: date) -> str:
    return str(d.weekday()) if d else ""

def _fmt_time(t) -> str:
    if not t:
        return ""
    if hasattr(t, "strftime"):
        try:
            return t.strftime("%H:%M")
        except Exception:
            pass
    return str(t)

@app.route("/exportar_lancamentos")
@admin_required
def exportar_lancamentos():
    import io
    from collections import defaultdict

    from flask import request, send_file
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill
    from openpyxl.utils import get_column_letter

    args = request.args

    restaurante_id = args.get("restaurante_id", type=int)
    cooperado_id = args.get("cooperado_id", type=int)

    data_inicio = _parse_date(args.get("data_inicio"))
    data_fim = _parse_date(args.get("data_fim"))

    dows = set(args.getlist("dow"))

    from datetime import date, timedelta

    hoje = date.today()
    inicio_semana = hoje - timedelta(days=hoje.weekday())
    fim_semana = inicio_semana + timedelta(days=6)

    if args.get("data_inicio") is None and args.get("data_fim") is None:
        data_inicio = inicio_semana
        data_fim = fim_semana

    q = Lancamento.query

    if restaurante_id:
        q = q.filter(Lancamento.restaurante_id == restaurante_id)

    if cooperado_id:
        q = q.filter(Lancamento.cooperado_id == cooperado_id)

    if data_inicio:
        q = q.filter(Lancamento.data >= data_inicio)

    if data_fim:
        q = q.filter(Lancamento.data <= data_fim)

    lancs = (
        q.order_by(
            Lancamento.data.desc(),
            Lancamento.id.desc()
        ).all()
    )

    if dows:
        lancs = [
            l for l in lancs
            if l.data and str(_dow(l.data)) in dows
        ]

    wb = Workbook()

    bold = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center")
    header_fill = PatternFill("solid", fgColor="DDDDDD")

    currency_fmt = "#,##0.00"
    date_fmt = "DD/MM/YYYY"

    def _style_header(ws, ncols: int):
        for col_idx in range(1, ncols + 1):
            cell = ws.cell(row=1, column=col_idx)
            cell.font = bold
            cell.alignment = center
            cell.fill = header_fill

    def _autosize(ws, max_col, max_row, cap=55):
        widths = [0] * (max_col + 1)
        for r in range(1, max_row + 1):
            for c in range(1, max_col + 1):
                v = ws.cell(r, c).value
                if v is None:
                    continue
                s = str(v)
                if len(s) > widths[c]:
                    widths[c] = len(s)
        for c in range(1, max_col + 1):
            ws.column_dimensions[get_column_letter(c)].width = min(max(10, widths[c] + 2), cap)

    ws_det = wb.active
    ws_det.title = "Lançamentos"

    header_det = [
        "Restaurante", "Periodo", "Cooperado", "Descricao",
        "Valor", "Data", "HoraInicio", "HoraFim",
        "INSS", "SEST", "Encargos", "Liquido",
    ]
    ws_det.append(header_det)
    _style_header(ws_det, ncols=len(header_det))

    totais_contrato = defaultdict(lambda: {
        "restaurante": "", "periodo": "",
        "bruto": 0.0, "inss": 0.0, "sest": 0.0, "enc": 0.0, "liq": 0.0,
    })
    totais_contrato_coop = defaultdict(lambda: {
        "restaurante": "", "periodo": "", "cooperado": "",
        "bruto": 0.0, "inss": 0.0, "sest": 0.0, "enc": 0.0, "liq": 0.0,
    })
    totais_coop = defaultdict(lambda: {
        "cooperado": "",
        "bruto": 0.0, "inss": 0.0, "sest": 0.0, "enc": 0.0, "liq": 0.0,
    })
    totais_coop_dia = defaultdict(lambda: {
        "cooperado": "", "data": None, "restaurante": "", "periodo": "",
        "bruto": 0.0, "inss": 0.0, "sest": 0.0, "enc": 0.0, "liq": 0.0,
    })

    total_geral_bruto = 0.0
    total_geral_inss = 0.0
    total_geral_sest = 0.0
    total_geral_enc = 0.0
    total_geral_liq = 0.0

    for l in lancs:
        v = float(l.valor or 0.0)

        inss = v * 0.04
        sest = v * 0.005
        encargos = inss + sest
        liq = v - encargos

        rest_nome = l.restaurante.nome if getattr(l, "restaurante", None) else ""
        rest_period = l.restaurante.periodo if getattr(l, "restaurante", None) else ""
        rest_id = int(getattr(l, "restaurante_id", 0) or 0)

        coop_nome = l.cooperado.nome if getattr(l, "cooperado", None) else ""
        coop_id = int(getattr(l, "cooperado_id", 0) or 0)

        row = [
            rest_nome,
            rest_period,
            coop_nome,
            (l.descricao or ""),
            v,
            l.data,
            _fmt_time(getattr(l, "hora_inicio", None)),
            _fmt_time(getattr(l, "hora_fim", None)),
            inss,
            sest,
            encargos,
            liq,
        ]
        ws_det.append(row)
        r = ws_det.max_row

        ws_det.cell(row=r, column=5).number_format = currency_fmt
        ws_det.cell(row=r, column=6).number_format = date_fmt
        ws_det.cell(row=r, column=9).number_format = currency_fmt
        ws_det.cell(row=r, column=10).number_format = currency_fmt
        ws_det.cell(row=r, column=11).number_format = currency_fmt
        ws_det.cell(row=r, column=12).number_format = currency_fmt

        key_contrato = (rest_id, rest_nome, rest_period)
        tc = totais_contrato[key_contrato]
        tc["restaurante"] = rest_nome
        tc["periodo"] = rest_period
        tc["bruto"] += v
        tc["inss"] += inss
        tc["sest"] += sest
        tc["enc"] += encargos
        tc["liq"] += liq

        key_contrato_coop = (rest_id, rest_nome, rest_period, coop_id, coop_nome)
        tcc = totais_contrato_coop[key_contrato_coop]
        tcc["restaurante"] = rest_nome
        tcc["periodo"] = rest_period
        tcc["cooperado"] = coop_nome
        tcc["bruto"] += v
        tcc["inss"] += inss
        tcc["sest"] += sest
        tcc["enc"] += encargos
        tcc["liq"] += liq

        key_coop = (coop_id, coop_nome)
        tcg = totais_coop[key_coop]
        tcg["cooperado"] = coop_nome
        tcg["bruto"] += v
        tcg["inss"] += inss
        tcg["sest"] += sest
        tcg["enc"] += encargos
        tcg["liq"] += liq

        key_coop_dia = (coop_id, coop_nome, l.data, rest_id, rest_nome, rest_period)
        tcd = totais_coop_dia[key_coop_dia]
        tcd["cooperado"] = coop_nome
        tcd["data"] = l.data
        tcd["restaurante"] = rest_nome
        tcd["periodo"] = rest_period
        tcd["bruto"] += v
        tcd["inss"] += inss
        tcd["sest"] += sest
        tcd["enc"] += encargos
        tcd["liq"] += liq

        total_geral_bruto += v
        total_geral_inss += inss
        total_geral_sest += sest
        total_geral_enc += encargos
        total_geral_liq += liq

    ws_det.freeze_panes = "A2"
    ws_det.auto_filter.ref = f"A1:{get_column_letter(len(header_det))}{ws_det.max_row}"
    _autosize(ws_det, max_col=len(header_det), max_row=min(ws_det.max_row, 3000))

    ws_con = wb.create_sheet("Totais por Contrato")
    header_contrato = [
        "Restaurante", "Periodo",
        "Total Bruto", "Total INSS", "Total SEST", "Total Encargos", "Total Líquido"
    ]
    ws_con.append(header_contrato)
    _style_header(ws_con, ncols=len(header_contrato))

    soma_b = soma_inss = soma_sest = soma_enc = soma_l = 0.0
    row_idx = 2

    for _, tc in sorted(
        totais_contrato.items(),
        key=lambda x: (x[1]["restaurante"], x[1]["periodo"])
    ):
        ws_con.append([
            tc["restaurante"] or "—",
            tc["periodo"] or "—",
            tc["bruto"],
            tc["inss"],
            tc["sest"],
            tc["enc"],
            tc["liq"],
        ])
        r = row_idx
        for col in (3, 4, 5, 6, 7):
            ws_con.cell(row=r, column=col).number_format = currency_fmt

        soma_b += tc["bruto"]
        soma_inss += tc["inss"]
        soma_sest += tc["sest"]
        soma_enc += tc["enc"]
        soma_l += tc["liq"]
        row_idx += 1

    if row_idx > 2:
        ws_con.append(["TOTAL GERAL", "", soma_b, soma_inss, soma_sest, soma_enc, soma_l])
        r = row_idx
        for col in (1, 3, 4, 5, 6, 7):
            cell = ws_con.cell(row=r, column=col)
            cell.font = bold
            if col != 1:
                cell.number_format = currency_fmt

    ws_con.freeze_panes = "A2"
    ws_con.auto_filter.ref = f"A1:{get_column_letter(len(header_contrato))}{ws_con.max_row}"
    _autosize(ws_con, max_col=len(header_contrato), max_row=ws_con.max_row)

    ws_cc = wb.create_sheet("Contrato x Cooperado")
    header_cc = [
        "Restaurante", "Periodo", "Cooperado",
        "Total Bruto", "Total INSS", "Total SEST", "Total Encargos", "Total Líquido"
    ]
    ws_cc.append(header_cc)
    _style_header(ws_cc, ncols=len(header_cc))

    row_idx = 2
    for _, tcc in sorted(
        totais_contrato_coop.items(),
        key=lambda x: (x[1]["restaurante"], x[1]["periodo"], x[1]["cooperado"])
    ):
        ws_cc.append([
            tcc["restaurante"] or "—",
            tcc["periodo"] or "—",
            tcc["cooperado"] or "—",
            tcc["bruto"],
            tcc["inss"],
            tcc["sest"],
            tcc["enc"],
            tcc["liq"],
        ])
        r = row_idx
        for col in (4, 5, 6, 7, 8):
            ws_cc.cell(row=r, column=col).number_format = currency_fmt
        row_idx += 1

    ws_cc.freeze_panes = "A2"
    ws_cc.auto_filter.ref = f"A1:{get_column_letter(len(header_cc))}{ws_cc.max_row}"
    _autosize(ws_cc, max_col=len(header_cc), max_row=ws_cc.max_row)

    ws_cd = wb.create_sheet("Cooperado por Dia")
    header_cd = [
        "Cooperado", "Data", "Restaurante", "Periodo",
        "Total Bruto", "Total INSS", "Total SEST", "Total Encargos", "Total Líquido"
    ]
    ws_cd.append(header_cd)
    _style_header(ws_cd, ncols=len(header_cd))

    row_idx = 2
    for _, tcd in sorted(
        totais_coop_dia.items(),
        key=lambda x: (x[1]["cooperado"], x[1]["data"] or date.min, x[1]["restaurante"], x[1]["periodo"])
    ):
        ws_cd.append([
            tcd["cooperado"] or "—",
            tcd["data"],
            tcd["restaurante"] or "—",
            tcd["periodo"] or "—",
            tcd["bruto"],
            tcd["inss"],
            tcd["sest"],
            tcd["enc"],
            tcd["liq"],
        ])
        r = row_idx
        ws_cd.cell(row=r, column=2).number_format = date_fmt
        for col in (5, 6, 7, 8, 9):
            ws_cd.cell(row=r, column=col).number_format = currency_fmt
        row_idx += 1

    ws_cd.freeze_panes = "A2"
    ws_cd.auto_filter.ref = f"A1:{get_column_letter(len(header_cd))}{ws_cd.max_row}"
    _autosize(ws_cd, max_col=len(header_cd), max_row=ws_cd.max_row)

    ws_tc = wb.create_sheet("Totais por Cooperado")
    header_tc = ["Cooperado", "Total Bruto", "Total INSS", "Total SEST", "Total Encargos", "Total Líquido"]
    ws_tc.append(header_tc)
    _style_header(ws_tc, ncols=len(header_tc))

    row_idx = 2
    for _, tcg in sorted(totais_coop.items(), key=lambda x: x[1]["cooperado"]):
        ws_tc.append([
            tcg["cooperado"] or "—",
            tcg["bruto"],
            tcg["inss"],
            tcg["sest"],
            tcg["enc"],
            tcg["liq"],
        ])
        r = row_idx
        for col in (2, 3, 4, 5, 6):
            ws_tc.cell(row=r, column=col).number_format = currency_fmt
        row_idx += 1

    if row_idx > 2:
        total_b = sum(v["bruto"] for v in totais_coop.values())
        total_i = sum(v["inss"] for v in totais_coop.values())
        total_s = sum(v["sest"] for v in totais_coop.values())
        total_e = sum(v["enc"] for v in totais_coop.values())
        total_l = sum(v["liq"] for v in totais_coop.values())
        ws_tc.append(["TOTAL GERAL", total_b, total_i, total_s, total_e, total_l])
        r = row_idx
        for col in (1, 2, 3, 4, 5, 6):
            cell = ws_tc.cell(row=r, column=col)
            cell.font = bold
            if col != 1:
                cell.number_format = currency_fmt

    ws_tc.freeze_panes = "A2"
    ws_tc.auto_filter.ref = f"A1:{get_column_letter(len(header_tc))}{ws_tc.max_row}"
    _autosize(ws_tc, max_col=len(header_tc), max_row=ws_tc.max_row)

    ws_rg = wb.create_sheet("Resumo Geral")
    ws_rg["A1"] = "Total Geral Bruto"
    ws_rg["A2"] = "Total Geral INSS"
    ws_rg["A3"] = "Total Geral SEST"
    ws_rg["A4"] = "Total Geral Encargos"
    ws_rg["A5"] = "Total Geral Líquido"
    for a in ("A1", "A2", "A3", "A4", "A5"):
        ws_rg[a].font = bold

    ws_rg["B1"] = total_geral_bruto
    ws_rg["B2"] = total_geral_inss
    ws_rg["B3"] = total_geral_sest
    ws_rg["B4"] = total_geral_enc
    ws_rg["B5"] = total_geral_liq
    for b in ("B1", "B2", "B3", "B4", "B5"):
        ws_rg[b].number_format = currency_fmt

    ws_rg.column_dimensions["A"].width = 24
    ws_rg.column_dimensions["B"].width = 18

    mem = io.BytesIO()
    wb.save(mem)
    mem.seek(0)

    return send_file(
        mem,
        as_attachment=True,
        download_name="lancamentos.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# =========================
# CRUD Lançamentos (Admin)
# =========================
@app.route("/admin/lancamentos/add", methods=["POST"])
@admin_required
def admin_add_lancamento():
    f = request.form
    l = Lancamento(
        restaurante_id=f.get("restaurante_id", type=int),
        cooperado_id=f.get("cooperado_id", type=int),
        descricao=f.get("descricao", "").strip(),
        valor=f.get("valor", type=float),
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
    l.descricao = f.get("descricao", "").strip()
    l.valor = f.get("valor", type=float)
    l.data = _parse_date(f.get("data"))
    l.hora_inicio = f.get("hora_inicio")
    l.hora_fim = f.get("hora_fim")
    l.qtd_entregas = f.get("qtd_entregas", type=int)
    db.session.commit()
    flash("Lançamento atualizado.", "success")
    return redirect(url_for("admin_lancamentos_split"))

@app.route("/admin/lancamentos/<int:id>/delete")
@admin_required
def admin_delete_lancamento(id):
    l = Lancamento.query.get_or_404(id)

    db.session.execute(sa_delete(AvaliacaoCooperado).where(AvaliacaoCooperado.lancamento_id == id))
    db.session.execute(sa_delete(AvaliacaoRestaurante).where(AvaliacaoRestaurante.lancamento_id == id))

    db.session.delete(l)
    db.session.commit()
    flash("Lançamento excluído.", "success")
    return redirect(url_for("admin_lancamentos_split"))

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

    def col(*names):
        for n in names:
            if hasattr(Model, n):
                return getattr(Model, n)
        return None

    f_geral = col("estrelas_geral")

    if tipo == "restaurante":
        f_trat = col("estrelas_tratamento", "estrelas_pontualidade")
        f_amb = col("estrelas_ambiente", "estrelas_educacao")
        f_sup = col("estrelas_suporte", "estrelas_eficiencia")
    else:
        f_pont = col("estrelas_pontualidade")
        f_educ = col("estrelas_educacao")
        f_efic = col("estrelas_eficiencia")
        f_apres = col("estrelas_apresentacao")

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

    page = request.args.get("page", type=int) or 1
    per_page = request.args.get("per_page", type=int) or 10000

    if per_page > 200:
        per_page = 200

    offset = (page - 1) * per_page

    rows = (
        base.order_by(Model.criado_em.desc())
        .limit(per_page)
        .offset(offset)
        .all()
    )

    pages = max(1, (total + per_page - 1) // per_page)

    pager = SimpleNamespace(
        page=page,
        per_page=per_page,
        total=total,
        pages=pages,
        has_prev=(page > 1),
        has_next=(page < pages)
    )

    avaliacoes = []
    for a, rest_id, rest_nome, coop_id, coop_nome in rows:
        item = {
            "criado_em": a.criado_em,
            "rest_id": rest_id,
            "rest_nome": rest_nome,
            "coop_id": coop_id,
            "coop_nome": coop_nome,
            "geral": getattr(a, "estrelas_geral", 0) or 0,
            "comentario": (getattr(a, "comentario", "") or "").strip(),
            "media": getattr(a, "media_ponderada", None),
            "sentimento": getattr(a, "sentimento", None),
            "temas": getattr(a, "temas", None),
            "alerta": bool(getattr(a, "alerta_crise", False)),
            "tratamento": 0, "ambiente": 0, "suporte": 0,
            "trat": 0, "amb": 0, "sup": 0,
            "pont": 0, "educ": 0, "efic": 0, "apres": 0,
        }

        if tipo == "restaurante":
            trat = getattr(a, "estrelas_tratamento", None)
            if trat is None:
                trat = getattr(a, "estrelas_pontualidade", 0)
            amb = getattr(a, "estrelas_ambiente", None)
            if amb is None:
                amb = getattr(a, "estrelas_educacao", 0)
            sup = getattr(a, "estrelas_suporte", None)
            if sup is None:
                sup = getattr(a, "estrelas_eficiencia", 0)

            item.update({
                "tratamento": trat or 0,
                "ambiente": amb or 0,
                "suporte": sup or 0,
                "trat": trat or 0,
                "amb": amb or 0,
                "sup": sup or 0,
            })
        else:
            item.update({
                "pont": getattr(a, "estrelas_pontualidade", 0) or 0,
                "educ": getattr(a, "estrelas_educacao", 0) or 0,
                "efic": getattr(a, "estrelas_eficiencia", 0) or 0,
                "apres": getattr(a, "estrelas_apresentacao", 0) or 0,
            })

        avaliacoes.append(SimpleNamespace(**item))

    def avg_or_zero(coluna):
        if coluna is None:
            return 0.0
        q = db.session.query(func.coalesce(func.avg(coluna), 0.0))
        if filtros:
            q = q.filter(and_(*filtros))
        return float(q.scalar() or 0.0)

    kpis = {"qtd": total, "geral": avg_or_zero(f_geral)}
    if tipo == "restaurante":
        kpis.update({
            "trat": avg_or_zero(f_trat),
            "amb": avg_or_zero(f_amb),
            "sup": avg_or_zero(f_sup),
        })
    else:
        kpis.update({
            "pont": avg_or_zero(f_pont),
            "educ": avg_or_zero(f_educ),
            "efic": avg_or_zero(f_efic),
            "apres": avg_or_zero(f_apres),
        })

    ranking, chart_top = [], {"labels": [], "values": []}
    if tipo == "restaurante":
        q_rank = (
            db.session.query(
                Restaurante.id.label("id"),
                Restaurante.nome.label("nome"),
                func.count(Model.id).label("qtd"),
                func.coalesce(func.avg(f_geral), 0.0).label("m_geral"),
                (func.coalesce(func.avg(f_trat), 0.0) if f_trat is not None else literal(0.0)).label("m_trat"),
                (func.coalesce(func.avg(f_amb), 0.0) if f_amb is not None else literal(0.0)).label("m_amb"),
                (func.coalesce(func.avg(f_sup), 0.0) if f_sup is not None else literal(0.0)).label("m_sup"),
            )
            .join(Model, Model.restaurante_id == Restaurante.id)
        )
        if filtros:
            q_rank = q_rank.filter(and_(*filtros))
        ranking_rows = q_rank.group_by(Restaurante.id, Restaurante.nome).all()
        ranking = [{
            "rest_nome": r.nome, "qtd": int(r.qtd or 0),
            "m_geral": float(r.m_geral or 0),
            "m_trat": float(r.m_trat or 0),
            "m_amb": float(r.m_amb or 0),
            "m_sup": float(r.m_sup or 0),
        } for r in ranking_rows]
        top = sorted([x for x in ranking if x["qtd"] >= 3], key=lambda x: x["m_geral"], reverse=True)[:10]
        chart_top = {"labels": [r["rest_nome"] for r in top], "values": [round(r["m_geral"], 2) for r in top]}
    else:
        q_rank = (
            db.session.query(
                Cooperado.id.label("id"),
                Cooperado.nome.label("nome"),
                func.count(Model.id).label("qtd"),
                func.coalesce(func.avg(f_geral), 0.0).label("m_geral"),
                (func.coalesce(func.avg(f_pont), 0.0) if f_pont is not None else literal(0.0)).label("m_pont"),
                (func.coalesce(func.avg(f_educ), 0.0) if f_educ is not None else literal(0.0)).label("m_educ"),
                (func.coalesce(func.avg(f_efic), 0.0) if f_efic is not None else literal(0.0)).label("m_efic"),
                (func.coalesce(func.avg(f_apres), 0.0) if f_apres is not None else literal(0.0)).label("m_apres"),
            )
            .join(Model, Model.cooperado_id == Cooperado.id)
        )
        if filtros:
            q_rank = q_rank.filter(and_(*filtros))
        ranking_rows = q_rank.group_by(Cooperado.id, Cooperado.nome).all()
        ranking = [{
            "coop_nome": r.nome, "qtd": int(r.qtd or 0),
            "m_geral": float(r.m_geral or 0),
            "m_pont": float(r.m_pont or 0),
            "m_educ": float(r.m_educ or 0),
            "m_efic": float(r.m_efic or 0),
            "m_apres": float(r.m_apres or 0),
        } for r in ranking_rows]
        top = sorted([x for x in ranking if x["qtd"] >= 3], key=lambda x: x["m_geral"], reverse=True)[:10]
        chart_top = {"labels": [r["coop_nome"] for r in top], "values": [round(r["m_geral"], 2) for r in top]}

    compat_map = {}
    for a in avaliacoes:
        key = (a.coop_id, a.rest_id)
        d = compat_map.get(key)
        if not d:
            d = {"coop": a.coop_nome, "rest": a.rest_nome, "sum": 0.0, "count": 0}
        d["sum"] += (a.geral or 0)
        d["count"] += 1
        compat_map[key] = d

    compat = []
    for d in compat_map.values():
        avg = (d["sum"] / d["count"]) if d["count"] else 0.0
        compat.append({"coop": d["coop"], "rest": d["rest"], "avg": avg, "count": d["count"]})
    compat.sort(key=lambda x: (-(x["avg"] or 0), -(x["count"] or 0), x["coop"], x["rest"]))

    _flt = SimpleNamespace(
        restaurante_id=restaurante_id,
        cooperado_id=cooperado_id,
        data_inicio=data_inicio or "",
        data_fim=data_fim or "",
    )
    preserve = request.args.to_dict(flat=True)
    preserve.pop("page", None)

    cfg = get_config()
    admin_user = Usuario.query.filter_by(tipo="admin").first()

    return render_template(
        "admin_avaliacoes.html",
        aba_ativa="avaliacoes",
        tab="avaliacoes",
        tipo=tipo,
        avaliacoes=avaliacoes,
        kpis=kpis,
        ranking=ranking,
        chart_top=chart_top,
        compat=compat,
        _flt=_flt,
        restaurantes=Restaurante.query.order_by(Restaurante.nome).all(),
        cooperados=Cooperado.query.order_by(Cooperado.nome).all(),
        pager=pager,
        page=pager.page,
        per_page=pager.per_page,
        preserve=preserve,
        admin=admin_user,
        salario_minimo=cfg.salario_minimo or 0.0,
    )

@app.route("/admin/avaliacoes/export")
@role_required("admin")
def admin_export_avaliacoes_csv():
    from flask import flash, redirect, url_for, request

    flash("Exportação em CSV ainda não foi implementada.", "warning")

    args = {
        "data_inicio": request.args.get("data_inicio") or "",
        "data_fim": request.args.get("data_fim") or "",
        "tipo": request.args.get("tipo") or "",
    }
    return redirect(url_for("admin_avaliacoes", **args))

# =========================
# PORTAL RESTAURANTE
# =========================
@app.route("/portal/restaurante")
@role_required("restaurante")
def portal_restaurante():
    from datetime import date, timedelta
    import re
    from werkzeug.routing import BuildError

    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    if not rest:
        return (
            "<p style='font-family:Arial;margin:40px'>"
            "Seu usuário não está vinculado a um estabelecimento. Avise o administrador."
            "</p>"
        )

    # Abas/visões: 'lancar', 'escalas', 'lancamentos', 'config', 'avisos'
    view = (request.args.get("view", "lancar") or "lancar").strip().lower()

    # ---- helper mês YYYY-MM
    def _parse_yyyy_mm_local(s: str):
        if not s:
            return None, None
        m = re.fullmatch(r"(\d{4})-(\d{2})", s.strip())
        if not m:
            return None, None
        y = int(m.group(1))
        mth = int(m.group(2))
        try:
            di_ = date(y, mth, 1)
            if mth == 12:
                df_ = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                df_ = date(y, mth + 1, 1) - timedelta(days=1)
            return di_, df_
        except Exception:
            return None, None

    # -------------------- LANÇAMENTOS (totais por período) --------------------
    di = _parse_date(request.args.get("data_inicio"))
    df = _parse_date(request.args.get("data_fim"))

    # NOVO: filtro por mês (?mes=YYYY-MM)
    mes = (request.args.get("mes") or "").strip()
    periodo_desc = None
    if mes:
        di_mes, df_mes = _parse_yyyy_mm_local(mes)
        if di_mes and df_mes:
            di, df = di_mes, df_mes
            periodo_desc = "mês"

    if not di or not df:
        # Sem filtro => janela semanal baseada no período do restaurante
        wd_map = {"seg-dom": 0, "sab-sex": 5, "sex-qui": 4}  # seg=0 ... dom=6
        start_wd = wd_map.get(getattr(rest, "periodo", None), 0)
        hoje = date.today()
        delta = (hoje.weekday() - start_wd) % 7
        di_auto = hoje - timedelta(days=delta)
        df_auto = di_auto + timedelta(days=6)
        di = di or di_auto
        df = df or df_auto
        periodo_desc = periodo_desc or getattr(rest, "periodo", "seg-dom")
    else:
        periodo_desc = periodo_desc or "personalizado"

    cooperados = Cooperado.query.order_by(Cooperado.nome).all()

    total_bruto = 0.0
    total_qtd = 0
    total_entregas = 0

    # 4% INSS + 0,5% SEST/SENAT (separados)
    total_inss = 0.0
    total_sest = 0.0

    for c in cooperados:
        q = (
            Lancamento.query
            .filter_by(restaurante_id=rest.id, cooperado_id=c.id)
            .filter(Lancamento.data >= di, Lancamento.data <= df)
            .order_by(Lancamento.data.desc(), Lancamento.id.desc())
        )
        c.lancamentos = q.all()

        c.total_periodo = sum((l.valor or 0.0) for l in c.lancamentos)
        c.inss_periodo = sum((l.valor or 0.0) * 0.04 for l in c.lancamentos)
        c.sest_periodo = sum((l.valor or 0.0) * 0.005 for l in c.lancamentos)

        c.encargos_periodo = c.inss_periodo + c.sest_periodo
        c.liquido_periodo = c.total_periodo - c.encargos_periodo

        total_bruto += c.total_periodo
        total_qtd += len(c.lancamentos)
        total_entregas += sum((l.qtd_entregas or 0) for l in c.lancamentos)

        total_inss += c.inss_periodo
        total_sest += c.sest_periodo

    total_encargos = total_inss + total_sest
    total_liquido = total_bruto - total_encargos

    # -------------------- ESCALA (Quem trabalha) --------------------
    def contrato_bate_restaurante(contrato: str, rest_nome: str) -> bool:
        a = " ".join(_normalize_name(contrato or ""))
        b = " ".join(_normalize_name(rest_nome or ""))
        if not a or not b:
            return False
        return a == b or a in b or b in a

    ref = _parse_date(request.args.get("ref")) or date.today()
    modo = request.args.get("modo", "semana")  # 'semana' ou 'dia'
    if modo == "dia":
        dias_list = [ref]
    else:
        semana_inicio = ref - timedelta(days=ref.weekday())
        dias_list = [semana_inicio + timedelta(days=i) for i in range(7)]

    escalas_all = Escala.query.order_by(Escala.id.asc()).all()
    eff_map = _carry_forward_contrato(escalas_all)

    escalas_rest = [
        e for e in escalas_all
        if contrato_bate_restaurante(eff_map.get(e.id, e.contrato or ""), rest.nome)
    ]
    if not escalas_rest:
        escalas_rest = [
            e for e in escalas_all
            if (e.contrato or "").strip() == (rest.nome or "").strip()
        ]

    agenda = {d: [] for d in dias_list}
    seen = {d: set() for d in dias_list}  # evita duplicar

    for e in escalas_rest:
        dt = _parse_data_escala_str(e.data)  # date | None
        wd = _weekday_from_data_str(e.data)  # 1..7 | None

        for d in dias_list:
            hit = (dt and dt == d) or (wd and wd == ((d.weekday() % 7) + 1))
            if not hit:
                continue

            coop = Cooperado.query.get(e.cooperado_id) if e.cooperado_id else None
            nome_fallback = (e.cooperado_nome or "").strip()
            nome_show = (coop.nome if coop else nome_fallback) or "—"
            contrato_eff = (eff_map.get(e.id, e.contrato or "") or "").strip()

            key = (
                (coop.id if coop else _norm(nome_show)),
                _norm(e.turno),
                _norm(e.horario),
                _norm(contrato_eff),
            )
            if key in seen[d]:
                break

            seen[d].add(key)
            agenda[d].append({
                "coop": coop,
                "cooperado_nome": nome_fallback or None,
                "nome_planilha": nome_show,
                "turno": (e.turno or "").strip(),
                "horario": (e.horario or "").strip(),
                "contrato": contrato_eff,
                "cor": (e.cor or "").strip(),
            })
            break

    for d in dias_list:
        agenda[d].sort(
            key=lambda x: (
                (x["contrato"] or "").lower(),
                (x.get("nome_planilha") or (x["coop"].nome if x["coop"] else "")).lower(),
            )
        )

    # -------------------- Lista de lançamentos (aba "lancamentos") --------------------
    lancamentos_periodo = []
    total_lanc_valor = 0.0
    total_lanc_entregas = 0

    if view == "lancamentos":
        q = (
            db.session.query(Lancamento, Cooperado)
            .join(Cooperado, Cooperado.id == Lancamento.cooperado_id)
            .filter(
                Lancamento.restaurante_id == rest.id,
                Lancamento.data >= di,
                Lancamento.data <= df,
            )
            .order_by(Lancamento.data.asc(), Lancamento.id.asc())
        )
        for lanc, coop in q.all():
            item = {
                "id": lanc.id,
                "data": lanc.data.strftime("%d/%m/%Y") if lanc.data else "",
                "hora_inicio": (
                    lanc.hora_inicio if isinstance(lanc.hora_inicio, str)
                    else (lanc.hora_inicio.strftime("%H:%M") if lanc.hora_inicio else "")
                ),
                "hora_fim": (
                    lanc.hora_fim if isinstance(lanc.hora_fim, str)
                    else (lanc.hora_fim.strftime("%H:%M") if lanc.hora_fim else "")
                ),
                "qtd_entregas": lanc.qtd_entregas or 0,
                "valor": float(lanc.valor or 0.0),
                "descricao": (lanc.descricao or ""),
                "cooperado_id": coop.id,
                "cooperado_nome": coop.nome,
                "contrato_nome": rest.nome,
            }
            lancamentos_periodo.append(item)

        total_lanc_valor = sum(x["valor"] for x in lancamentos_periodo)
        total_lanc_entregas = sum(x["qtd_entregas"] for x in lancamentos_periodo)

    # ---- URLs/flags para template
    from werkzeug.routing import BuildError
    try:
        url_lancar_producao = url_for("lancar_producao")
    except BuildError:
        url_lancar_producao = "/restaurante/lancar_producao"

    has_editar_lanc = ("editar_lancamento" in app.view_functions)

    # -------------------- Render --------------------
    return render_template(
        "restaurante_dashboard.html",
        rest=rest,
        cooperados=cooperados,
        filtro_inicio=di,
        filtro_fim=df,
        filtro_mes=(mes or ""),
        periodo_desc=periodo_desc,
        total_bruto=total_bruto,
        total_inss=total_inss,
        total_sest=total_sest,
        total_encargos=total_encargos,
        total_liquido=total_liquido,
        total_qtd=total_qtd,
        total_entregas=total_entregas,
        view=view,
        agenda=agenda,
        dias_list=dias_list,
        ref_data=ref,
        modo=modo,
        lancamentos_periodo=(lancamentos_periodo if view == "lancamentos" else []),
        total_lanc_valor=total_lanc_valor,
        total_lanc_entregas=total_lanc_entregas,
        url_lancar_producao=url_lancar_producao,
        has_editar_lanc=has_editar_lanc,
    )

# =========================
# Rotas de CRUD de lançamento
# =========================
@app.post("/restaurante/lancar_producao")
@role_required("restaurante")
def lancar_producao():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first()
    if not rest:
        abort(403)
    f = request.form

    # NOVO: captura a descrição do formulário (somente para o estabelecimento)
    desc_raw = (f.get("descricao") or "").strip()
    desc_val = desc_raw or None  # salva None se vazio

    # 1) cria o lançamento
    l = Lancamento(
        restaurante_id=rest.id,
        cooperado_id=f.get("cooperado_id", type=int),
        descricao=desc_val,                            # <<< NOVO: salvar descrição
        valor=f.get("valor", type=float),
        data=_parse_date(f.get("data")) or date.today(),
        hora_inicio=f.get("hora_inicio"),
        hora_fim=f.get("hora_fim"),
        qtd_entregas=f.get("qtd_entregas", type=int),
    )
    db.session.add(l)
    db.session.flush()  # garante l.id

    # 2) avaliação (opcional)
    g   = _clamp_star(f.get("av_geral"))
    p   = _clamp_star(f.get("av_pontualidade"))
    ed  = _clamp_star(f.get("av_educacao"))
    ef  = _clamp_star(f.get("av_eficiencia"))
    ap  = _clamp_star(f.get("av_apresentacao"))
    txt = (f.get("av_comentario") or "").strip()

    tem_avaliacao = any(x is not None for x in (g, p, ed, ef, ap)) or bool(txt)
    if tem_avaliacao:
        media = _media_ponderada(g, p, ed, ef, ap)
        senti = _analise_sentimento(txt)
        temas = _identifica_temas(txt)
        crise = _sinaliza_crise(g, txt)
        feed  = _gerar_feedback(p, ed, ef, ap, txt, senti)

        av = AvaliacaoCooperado(
            restaurante_id=rest.id,
            cooperado_id=l.cooperado_id,
            lancamento_id=l.id,
            estrelas_geral=g,
            estrelas_pontualidade=p,
            estrelas_educacao=ed,
            estrelas_eficiencia=ef,
            estrelas_apresentacao=ap,
            comentario=txt,
            media_ponderada=media,
            sentimento=senti,
            temas="; ".join(temas),
            alerta_crise=crise,
            feedback_motoboy=feed,
        )
        db.session.add(av)
        if crise:
            flash("⚠️ Avaliação crítica registrada (1★ + termo de risco). A gerência deve revisar.", "danger")

    db.session.commit()
    flash("Produção lançada" + (" + avaliação salva." if tem_avaliacao else "."), "success")
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
        l.valor = f.get("valor", type=float)
        l.data = _parse_date(f.get("data")) or l.data
        l.hora_inicio = f.get("hora_inicio")
        l.hora_fim = f.get("hora_fim")
        l.qtd_entregas = f.get("qtd_entregas", type=int)
        # NOVO: permitir atualizar descrição se o form de edição trouxer o campo
        if "descricao" in f:
            l.descricao = (f.get("descricao") or "").strip() or None
        db.session.commit()
        flash("Lançamento atualizado.", "success")
        return redirect(url_for("portal_restaurante", view="lancamentos",
                                data_inicio=(l.data and l.data.strftime("%Y-%m-%d"))))

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

# =========================
# Documentos (Admin + Público)
# =========================
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

    # === NOVO: salva em diretório persistente e retorna NOME ÚNICO ===
    nome_unico = salvar_documento_upload(arquivo)
    if not nome_unico:
        flash("Falha ao salvar o arquivo.", "danger")
        return redirect(url_for("admin_documentos"))

    # compat: também guardamos um URL que aponta para /docs/<nome>
    d = Documento(
        titulo=titulo,
        categoria=categoria,
        descricao=descricao,
        arquivo_url=url_for("serve_documento", nome=nome_unico),  # compat com templates antigos
        arquivo_nome=nome_unico,  # agora guardamos o NOME ÚNICO persistido
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
        # === NOVO: tenta deletar do armazenamento persistente pelo nome salvo ===
        p = resolve_document_path(d.arquivo_nome)
        if p and os.path.exists(p):
            os.remove(p)
        # Fallback (legado): se sobrou um caminho em /static/uploads/docs/ no arquivo_url, remove também
        if d.arquivo_url and d.arquivo_url.startswith("/static/uploads/docs/"):
            legacy_path = os.path.join(BASE_DIR, d.arquivo_url.lstrip("/"))
            if os.path.exists(legacy_path):
                os.remove(legacy_path)
    except Exception:
        pass

    db.session.delete(d)
    db.session.commit()
    flash("Documento removido.", "success")
    return redirect(url_for("admin_documentos"))


@app.route("/documentos")
def documentos_publicos():
    uid = session.get("user_id")
    if not uid:
        return redirect(url_for("login"))
    documentos = Documento.query.order_by(Documento.enviado_em.desc()).all()
    return render_template("documentos_publicos.html", documentos=documentos)


@app.route('/documentos/<int:doc_id>/baixar')
def baixar_documento(doc_id):
    doc = Documento.query.get_or_404(doc_id)
    # === NOVO: resolve caminho persistente pelo nome salvo ===
    path = resolve_document_path(doc.arquivo_nome)
    if not path or not os.path.exists(path):
        abort(404)
    # força download (independente do tipo)
    return send_file(
        path,
        as_attachment=True,
        download_name=os.path.basename(doc.arquivo_nome)
    )

# =========================
# Inicialização automática do DB em servidores (Gunicorn/Render)
# =========================
try:
    with app.app_context():
        init_db()
except Exception as _e:
    # Evita crash no import; logs úteis no servidor
    try:
        app.logger.warning(f"Falha ao inicializar DB: {_e}")
    except Exception:
        pass


@app.errorhandler(413)
def too_large(e):
    flash("Arquivo excede o tamanho máximo permitido (32MB).", "danger")
    return redirect(url_for('admin_documentos'))

# =========================
# Inicialização automática do DB em servidores (Gunicorn/Render)
# =========================
try:
    with app.app_context():
        init_db()
except Exception as _e:
    ...

# ==== TABELAS (upload/abrir/baixar) =========================================
from flask import (
    render_template, request, redirect, url_for, flash, session,
    send_file, abort, current_app, jsonify
)
from werkzeug.utils import secure_filename
from datetime import datetime
from pathlib import Path
import os, re, unicodedata, mimetypes, logging

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BASE_DIR e TABELAS_DIR (sempre salva/serve de static/uploads/tabelas)
# ---------------------------------------------------------------------------
try:
    BASE_DIR  # type: ignore[name-defined]
except NameError:
    BASE_DIR = Path(__file__).resolve().parent

# SEMPRE neste local:
TABELAS_DIR = str(Path(BASE_DIR) / "static" / "uploads" / "tabelas")

# Requer no app principal:
# - db (SQLAlchemy)
# - modelos: Tabela(id, titulo, descricao?, arquivo_url, arquivo_nome?, enviado_em)
#            Restaurante(usuario_id, nome?, usuario?, usuario_ref?.usuario?)
# - decorators: admin_required, role_required (se usar avisos/portais)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _tabelas_base_dir() -> Path:
    p = Path(TABELAS_DIR)
    p.mkdir(parents=True, exist_ok=True)
    return p

def _norm_txt(s: str) -> str:
    s = unicodedata.normalize("NFD", (s or "").strip())
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = re.sub(r"\s+", " ", s)
    return s.lower()

def _guess_mimetype_from_path(path: str) -> str:
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"

def _enforce_restaurante_titulo(tabela, restaurante):
    """
    Regra: restaurante só acessa a tabela cujo TÍTULO == NOME/LOGIN do restaurante (normalizado).
    """
    login_nome = (
        getattr(getattr(restaurante, "usuario_ref", None), "usuario", None)
        or getattr(restaurante, "usuario", None)
        or (restaurante.nome or "")
    )
    if _norm_txt(tabela.titulo) != _norm_txt(login_nome):
        abort(403)

def _serve_tabela_or_redirect(tabela, *, as_attachment: bool):
    """
    Resolve e serve o arquivo da Tabela:
    - http(s) => redirect
    - sempre tenta primeiro static/uploads/tabelas/<arquivo>
    - aceita absoluto, relativo e só o nome
    - ignora querystring/fragments (ex.: foo.pdf?v=123#x)
    """
    url = (tabela.arquivo_url or "").strip()
    if not url:
        abort(404)

    # URL externa
    if url.startswith(("http://", "https://")):
        return redirect(url)

    base_dir    = Path(BASE_DIR)
    tabelas_dir = _tabelas_base_dir()

    # normaliza: remove "/" inicial, query e fragment
    raw = url.lstrip("/")
    raw_no_q = raw.split("?", 1)[0].split("#", 1)[0]
    fname = (raw_no_q.split("/")[-1] if raw_no_q else "").strip()

    candidates = []

    # 1) SEMPRE prioriza nosso diretório oficial
    if fname:
        candidates.append(tabelas_dir / fname)

    # 2) Como veio, relativo ao BASE_DIR (compat c/ legado: static/uploads/tabelas/...)
    candidates.append(base_dir / raw_no_q)

    # 3) Absoluto (se alguém gravou caminho completo por engano)
    p = Path(url)
    if p.is_absolute():
        candidates.append(p)

    # 4) Mais dois legados comuns
    if fname:
        candidates.append(base_dir / "uploads" / "tabelas" / fname)
        candidates.append(base_dir / "static" / "uploads" / "tabelas" / fname)

    file_path = next((c for c in candidates if c.exists() and c.is_file()), None)
    if not file_path:
        try:
            log.warning(
                "Arquivo de Tabela não encontrado. id=%s titulo=%r arquivo_url=%r tents=%r",
                getattr(tabela, "id", None),
                getattr(tabela, "titulo", None),
                tabela.arquivo_url,
                [str(c) for c in candidates],
            )
        except Exception:
            pass
        abort(404)

    return send_file(
        str(file_path),
        as_attachment=as_attachment,
        download_name=(tabela.arquivo_nome or file_path.name),
        mimetype=_guess_mimetype_from_path(str(file_path)),
    )

# ---------------------------------------------------------------------------
# Admin: listar / upload / delete
# ---------------------------------------------------------------------------
@app.get("/admin/tabelas", endpoint="admin_tabelas")
@admin_required
def admin_tabelas():
    tabelas = Tabela.query.order_by(Tabela.enviado_em.desc(), Tabela.id.desc()).all()
    return render_template("admin_tabelas.html", tabelas=tabelas)

@app.post("/admin/tabelas/upload", endpoint="admin_upload_tabela")
@admin_required
def admin_upload_tabela():
    f = request.form
    titulo    = (f.get("titulo") or "").strip()
    descricao = (f.get("descricao") or "").strip() or None

    # aceita vários nomes possíveis do input file
    arquivo = (
        request.files.get("arquivo")
        or request.files.get("file")
        or request.files.get("tabela")
    )

    if not titulo or not (arquivo and arquivo.filename):
        flash("Preencha o título e selecione o arquivo.", "warning")
        return redirect(url_for("admin_tabelas"))

    base_dir = _tabelas_base_dir()

    # nome seguro + timestamp pra não colidir
    raw = secure_filename(arquivo.filename)
    stem, ext = os.path.splitext(raw)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_stem = re.sub(r"[^A-Za-z0-9_.-]+", "-", stem) or "arquivo"
    final_name = f"{safe_stem}_{ts}{ext or ''}"

    dest = base_dir / final_name
    arquivo.save(str(dest))

    t = Tabela(
        titulo=titulo,
        descricao=descricao,
        # Importante: gravar apenas o NOME, não o caminho.
        # O _serve_tabela_or_redirect vai resolver para TABELAS_DIR.
        arquivo_url=final_name,
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
        url = (t.arquivo_url or "").strip()
        if url and not url.startswith(("http://", "https://")):
            path = _tabelas_base_dir() / (url.split("/")[-1])
            if path.exists():
                path.unlink()
    except Exception:
        pass
    db.session.delete(t)
    db.session.commit()
    flash("Tabela excluída.", "success")
    return redirect(url_for("admin_tabelas"))

# ---------------------------------------------------------------------------
# Cooperado/Admin/Restaurante: listagem (cooperado vê TODAS)
# Mantém endpoints esperados pelo HTML: 'tabela_abrir' e 'baixar_tabela'
# ---------------------------------------------------------------------------
@app.get("/tabelas", endpoint="tabelas_publicas")
def tabelas_publicas():
    if session.get("user_tipo") not in {"admin", "cooperado", "restaurante"}:
        return redirect(url_for("login"))

    tabs = Tabela.query.order_by(Tabela.enviado_em.desc(), Tabela.id.desc()).all()

    items = [{
        "id": t.id,
        "titulo": t.titulo,
        "descricao": getattr(t, "descricao", None),
        "enviado_em": t.enviado_em,
        "arquivo_nome": getattr(t, "arquivo_nome", None),
        "abrir_url":  url_for("tabela_abrir",  tab_id=t.id),
        "baixar_url": url_for("baixar_tabela", tab_id=t.id),
    } for t in tabs]

    back_href = url_for("portal_cooperado") if (
        session.get("user_tipo") == "cooperado" and "portal_cooperado" in current_app.view_functions
    ) else ""

    return render_template("tabelas_publicas.html", tabelas=tabs, items=items, back_href=back_href)

# ---------------------------------------------------------------------------
# Abrir / Baixar compartilhado (cooperado/admin/restaurante)
# Endpoints compatíveis com HTML existente:
#   - 'tabela_abrir'
#   - 'baixar_tabela'
# ---------------------------------------------------------------------------
@app.get("/tabelas/<int:tab_id>/abrir", endpoint="tabela_abrir")
def tabela_abrir(tab_id: int):
    if session.get("user_tipo") not in {"admin", "cooperado", "restaurante"}:
        return redirect(url_for("login"))
    t = Tabela.query.get_or_404(tab_id)

    if session.get("user_tipo") == "restaurante":
        rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
        _enforce_restaurante_titulo(t, rest)

    return _serve_tabela_or_redirect(t, as_attachment=False)

@app.get("/tabelas/<int:tab_id>/baixar", endpoint="baixar_tabela")
def tabela_baixar(tab_id: int):
    if session.get("user_tipo") not in {"admin", "cooperado", "restaurante"}:
        return redirect(url_for("login"))
    t = Tabela.query.get_or_404(tab_id)

    if session.get("user_tipo") == "restaurante":
        rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
        _enforce_restaurante_titulo(t, rest)

    return _serve_tabela_or_redirect(t, as_attachment=True)

# ---------------------------------------------------------------------------
# Restaurante: vê/abre/baixa SOMENTE a própria tabela
# Endpoints compatíveis com HTML existente:
#   - 'rest_tabela_abrir'
#   - 'rest_tabela_download'
# ---------------------------------------------------------------------------
@app.get("/rest/tabelas", endpoint="rest_tabelas")
def rest_tabelas():
    if session.get("user_tipo") != "restaurante":
        return redirect(url_for("login"))

    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first_or_404()

    login_nome = (
        getattr(getattr(rest, "usuario_ref", None), "usuario", None)
        or getattr(rest, "usuario", None)
        or (rest.nome or "")
    )
    alvo_norm = _norm_txt(login_nome)

    candidatos = Tabela.query.order_by(Tabela.enviado_em.desc()).all()
    tabela_exata = next((t for t in candidatos if _norm_txt(t.titulo) == alvo_norm), None)

    has_portal_restaurante = ("portal_restaurante" in current_app.view_functions)

    return render_template(
        "restaurantes_tabelas.html",
        restaurante=rest,
        login_nome=login_nome,
        tabela=tabela_exata,  # o template deve lidar com None (sem tabela)
        has_portal_restaurante=has_portal_restaurante,
        back_href=url_for("portal_restaurante") if has_portal_restaurante else url_for("rest_tabelas"),
        current_year=datetime.utcnow().year,
    )

@app.get("/rest/tabelas/<int:tabela_id>/abrir", endpoint="rest_tabela_abrir")
def rest_tabela_abrir(tabela_id: int):
    if session.get("user_tipo") != "restaurante":
        return redirect(url_for("login"))
    rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    t = Tabela.query.get_or_404(tabela_id)
    _enforce_restaurante_titulo(t, rest)
    return _serve_tabela_or_redirect(t, as_attachment=False)

@app.get("/rest/tabelas/<int:tabela_id>/download", endpoint="rest_tabela_download")
def rest_tabela_download(tabela_id: int):
    if session.get("user_tipo") != "restaurante":
        return redirect(url_for("login"))
    rest = Restaurante.query.filter_by(usuario_id=session.get("user_id")).first_or_404()
    t = Tabela.query.get_or_404(tabela_id)
    _enforce_restaurante_titulo(t, rest)
    return _serve_tabela_or_redirect(t, as_attachment=True)

# ---------------------------------------------------------------------------
# Diagnóstico rápido (admin)
# ---------------------------------------------------------------------------
@app.get("/admin/tabelas/scan", endpoint="admin_tabelas_scan")
@admin_required
def admin_tabelas_scan():
    base = _tabelas_base_dir()
    items = []
    for t in Tabela.query.order_by(Tabela.enviado_em.desc()).all():
        url = (t.arquivo_url or "").strip()
        # sempre tentar resolver como gravamos hoje (fname dentro de static/uploads/tabelas)
        fname = (url.split("?", 1)[0].split("#", 1)[0]).split("/")[-1] if url else ""
        resolved = str(base / fname) if fname else url
        exists = (Path(resolved).exists() if fname else False)
        items.append({
            "id": t.id,
            "titulo": t.titulo,
            "arquivo_nome": t.arquivo_nome,
            "arquivo_url": t.arquivo_url,
            "resolved": resolved,
            "exists": bool(exists),
        })
    return jsonify({"tabelas_dir": str(base), "items": items})

# ---------------------------------------------------------------------------
# (Opcional) Normalizador: deixa arquivo_url só com o NOME do arquivo
# Use se no seu banco ficaram caminhos tipo "static/uploads/tabelas/foo.pdf"
# ---------------------------------------------------------------------------
@app.post("/admin/tabelas/normalize-arquivo-url", endpoint="admin_tabelas_normalize_arquivo_url")
@admin_required
def admin_tabelas_normalize_arquivo_url():
    alterados = 0
    for t in Tabela.query.all():
        url = (t.arquivo_url or "").strip()
        if not url or url.startswith(("http://", "https://")):
            continue
        fname = url.split("?", 1)[0].split("#", 1)[0].split("/")[-1]
        if fname and fname != url:
            t.arquivo_url = fname
            alterados += 1
    if alterados:
        db.session.commit()
    return jsonify({"ok": True, "alterados": alterados})

# =========================
# AVISOS — Ações (cooperado)
# =========================

@app.post("/avisos/<int:aviso_id>/lido", endpoint="marcar_aviso_lido")
@role_required("cooperado")
def marcar_aviso_lido(aviso_id: int):
    u_id = session.get("user_id")
    coop = Cooperado.query.filter_by(usuario_id=u_id).first_or_404()

    aviso = Aviso.query.get_or_404(aviso_id)

    ja_lido = AvisoLeitura.query.filter_by(
        cooperado_id=coop.id, aviso_id=aviso.id
    ).first()

    if not ja_lido:
        db.session.add(AvisoLeitura(
            cooperado_id=coop.id,
            aviso_id=aviso.id,
            lido_em=datetime.utcnow(),
        ))
        db.session.commit()

    # volta para a lista; se quiser voltar ancorado: + f"#aviso-{aviso.id}"
    return redirect(url_for("portal_cooperado_avisos"))

@app.post("/avisos/marcar-todos", endpoint="marcar_todos_avisos_lidos")
@role_required("cooperado")
def marcar_todos_avisos_lidos():
    u_id = session.get("user_id")
    coop = Cooperado.query.filter_by(usuario_id=u_id).first_or_404()

    # todos avisos visíveis ao cooperado
    avisos = get_avisos_for_cooperado(coop)

    # ids já lidos
    lidos_ids = {
        a_id for (a_id,) in db.session.query(AvisoLeitura.aviso_id)
        .filter(AvisoLeitura.cooperado_id == coop.id).all()
    }

    # persiste só os que faltam
    now = datetime.utcnow()
    for a in avisos:
        if a.id not in lidos_ids:
            db.session.add(AvisoLeitura(
                cooperado_id=coop.id,
                aviso_id=a.id,
                lido_em=now,
            ))

    db.session.commit()
    return redirect(url_for("portal_cooperado_avisos"))

@app.get("/portal/restaurante/avisos")
@role_required("restaurante")
def portal_restaurante_avisos():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first_or_404()

    # avisos aplicáveis
    try:
        avisos_db = get_avisos_for_restaurante(rest)
    except NameError:
        # fallback: global + restaurante (associados ou broadcast)
        avisos_db = (Aviso.query
                     .filter(Aviso.ativo.is_(True))
                     .filter(or_(Aviso.tipo == "global", Aviso.tipo == "restaurante"))
                     .order_by(Aviso.fixado.desc(), Aviso.criado_em.desc())
                     .all())

    # ids já lidos
    lidos_ids = {
        a_id for (a_id,) in db.session.query(AvisoLeitura.aviso_id)
        .filter(AvisoLeitura.restaurante_id == rest.id).all()
    }

    def corpo_do_aviso(a: Aviso) -> str:
        for k in ("corpo_html","html","conteudo_html","mensagem_html","descricao_html","texto_html",
                  "corpo","conteudo","mensagem","descricao","texto","resumo","body","content"):
            v = getattr(a, k, None)
            if isinstance(v, str) and v.strip():
                return v
        return ""

    avisos = [{
        "id": a.id,
        "titulo": a.titulo or "Aviso",
        "criado_em": a.criado_em,
        "lido": (a.id in lidos_ids),
        "prioridade_alta": (str(a.prioridade or "").lower() == "alta"),
        "corpo_html": corpo_do_aviso(a),
    } for a in avisos_db]

    avisos_nao_lidos_count = sum(1 for x in avisos if not x["lido"])
    return render_template(
        "portal_restaurante_avisos.html",   # crie/clone seu template
        avisos=avisos,
        avisos_nao_lidos_count=avisos_nao_lidos_count,
        current_year=datetime.now().year,
    )

@app.post("/avisos-restaurante/marcar-todos", endpoint="marcar_todos_avisos_lidos_restaurante")
@role_required("restaurante")
def marcar_todos_avisos_lidos_restaurante():
    u_id = session.get("user_id")
    rest = Restaurante.query.filter_by(usuario_id=u_id).first_or_404()

    try:
        avisos = get_avisos_for_restaurante(rest)
    except NameError:
        avisos = (Aviso.query
                  .filter(Aviso.ativo.is_(True))
                  .filter(or_(Aviso.tipo == "global", Aviso.tipo == "restaurante"))
                  .all())

    lidos_ids = {
        a_id for (a_id,) in db.session.query(AvisoLeitura.aviso_id)
        .filter(AvisoLeitura.restaurante_id == rest.id).all()
    }

    now = datetime.utcnow()
    for a in avisos:
        if a.id not in lidos_ids:
            db.session.add(AvisoLeitura(
                restaurante_id=rest.id, aviso_id=a.id, lido_em=now
            ))
    db.session.commit()
    return redirect(url_for("portal_restaurante_avisos"))

# routes/avisos.py
from flask import Blueprint, jsonify
from flask_login import login_required, current_user

bp = Blueprint("avisos", __name__)

# aceita com e sem barra final, evitando 308/404 dependendo do strict_slashes
@app.get("/avisos/unread_count")
@app.get("/avisos/unread_count/")
def avisos_unread_count():
    """
    Retorna o número de avisos não lidos para o usuário atual.
    Mantém a lógica original (cooperado/restaurante) e melhora robustez/headers.
    """
    def _nocache(resp):
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return resp

    if "user_id" not in session:
        return _nocache(jsonify(count=0)), 401

    user_id = session.get("user_id")
    user_tipo = (session.get("user_tipo") or "").lower()

    try:
        count = 0

        if user_tipo == "cooperado":
            coop = Cooperado.query.filter_by(usuario_id=user_id).first()
            if not coop:
                return _nocache(jsonify(count=0)), 403

            avisos = list(get_avisos_for_cooperado(coop))
            # busca apenas os IDs já lidos (consulta mais leve)
            lidos_ids = {
                row[0] for row in
                db.session.query(AvisoLeitura.aviso_id).filter_by(cooperado_id=coop.id).all()
            }
            count = sum(1 for a in avisos if a.id not in lidos_ids)

        elif user_tipo == "restaurante":
            rest = Restaurante.query.filter_by(usuario_id=user_id).first()
            if not rest:
                return _nocache(jsonify(count=0)), 403

            avisos = list(get_avisos_for_restaurante(rest))
            lidos_ids = {
                row[0] for row in
                db.session.query(AvisoLeitura.aviso_id).filter_by(restaurante_id=rest.id).all()
            }
            count = sum(1 for a in avisos if a.id not in lidos_ids)

        # outros tipos: count = 0
        resp = jsonify(count=int(count))
        return _nocache(resp), 200

    except Exception:
        db.session.rollback()
        try:
            current_app.logger.exception("Erro ao calcular /avisos/unread_count")
        except Exception:
            pass
        return _nocache(jsonify(count=0)), 500

import click

@app.cli.command("init-db")
def init_db_command():
    """Roda init_db() manualmente."""
    click.echo("Rodando init_db() ...")
    with app.app_context():
        init_db()
    click.echo("init_db() concluído.")

# =========================
# Main
# =========================
if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    

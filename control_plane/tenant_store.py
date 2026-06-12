"""
Tenant Store — SQLite-backed tenant configuration.

Stores per-tenant settings:
  - API key (used for authentication)
  - Tier (premium | standard)
  - Rate limit (requests per second)
  - System prompt (injected by Inference Gateway)

Uses aiosqlite for async SQLite access.
In production, swap for asyncpg + PostgreSQL.
"""

import os
import logging
import aiosqlite
from typing import Optional

from shared.schemas import TenantConfig

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "/app/data/tenants.db")


async def init_db() -> None:
    """Creates the tenants table if it doesn't exist."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                tenant_id     TEXT PRIMARY KEY,
                api_key       TEXT UNIQUE NOT NULL,
                tier          TEXT NOT NULL DEFAULT 'standard',
                rate_limit_rps INTEGER NOT NULL DEFAULT 10,
                system_prompt TEXT NOT NULL DEFAULT '',
                created_at    REAL NOT NULL DEFAULT (unixepoch())
            )
        """)
        await db.commit()
        logger.info(f"Tenant database initialised at {DB_PATH}")

    # Seed default tenants for local development
    await _seed_default_tenants()


async def _seed_default_tenants() -> None:
    """Seeds two default tenants for local development convenience."""
    defaults = [
        TenantConfig(
            tenant_id="acme",
            api_key="key-acme-premium",
            tier="premium",
            rate_limit_rps=100,
            system_prompt="You are a helpful assistant for AcmeCorp. "
                          "Be concise and professional.",
        ),
        TenantConfig(
            tenant_id="beta-corp",
            api_key="key-beta-standard",
            tier="standard",
            rate_limit_rps=10,
            system_prompt="",
        ),
    ]
    for tenant in defaults:
        existing = await get_tenant(tenant.tenant_id)
        if not existing:
            await upsert_tenant(tenant)
            logger.info(f"Seeded default tenant: {tenant.tenant_id}")


async def get_tenant(tenant_id: str) -> Optional[TenantConfig]:
    """Fetches a tenant by tenant_id. Returns None if not found."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tenants WHERE tenant_id = ?", (tenant_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return TenantConfig(**dict(row))
            return None


async def get_tenant_by_key(api_key: str) -> Optional[TenantConfig]:
    """Fetches a tenant by API key. Used by the API Gateway for auth."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tenants WHERE api_key = ?", (api_key,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return TenantConfig(**dict(row))
            return None


async def upsert_tenant(tenant: TenantConfig) -> TenantConfig:
    """Creates or updates a tenant record."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO tenants
                (tenant_id, api_key, tier, rate_limit_rps, system_prompt)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(tenant_id) DO UPDATE SET
                api_key        = excluded.api_key,
                tier           = excluded.tier,
                rate_limit_rps = excluded.rate_limit_rps,
                system_prompt  = excluded.system_prompt
        """, (
            tenant.tenant_id,
            tenant.api_key,
            tenant.tier,
            tenant.rate_limit_rps,
            tenant.system_prompt,
        ))
        await db.commit()
        logger.info(f"Upserted tenant: {tenant.tenant_id}")
        return tenant


async def delete_tenant(tenant_id: str) -> bool:
    """Deletes a tenant. Returns True if a row was deleted."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM tenants WHERE tenant_id = ?", (tenant_id,)
        )
        await db.commit()
        return cursor.rowcount > 0


async def list_tenants() -> list[TenantConfig]:
    """Returns all tenants."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tenants") as cursor:
            rows = await cursor.fetchall()
            return [TenantConfig(**dict(row)) for row in rows]
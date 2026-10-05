from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def make_engine(url: str, pool_size: int = 10, max_overflow: int = 10) -> AsyncEngine:
    return create_async_engine(url, pool_size=pool_size, max_overflow=max_overflow, pool_pre_ping=True, pool_timeout=30)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)

"""initial schema

Revision ID: 0001
Revises:
Creates every table, constraint and index declared in app.models (the models are the single schema source for
this first revision; later revisions must be explicit op.* migrations).
"""
from alembic import op

import app.models  # noqa: F401
from app.db.base import Base

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    Base.metadata.create_all(bind=op.get_bind())


def downgrade() -> None:
    Base.metadata.drop_all(bind=op.get_bind())

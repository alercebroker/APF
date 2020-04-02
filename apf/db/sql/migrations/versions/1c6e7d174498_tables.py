"""tables

Revision ID: 1c6e7d174498
Revises: 4d6955a953a5
Create Date: 2020-04-01 15:14:05.440821

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1c6e7d174498'
down_revision = '4d6955a953a5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('outlier_detector',
    sa.Column('name', sa.String(), nullable=False),
    sa.PrimaryKeyConstraint('name')
    )
    op.create_table('outlier_score',
    sa.Column('astro_object', sa.String(), nullable=False),
    sa.Column('detector_name', sa.String(), nullable=False),
    sa.Column('score', sa.Float(), nullable=False),
    sa.Column('scores', sa.JSON(), nullable=True),
    sa.ForeignKeyConstraint(['astro_object'], ['astro_object.oid'], ),
    sa.ForeignKeyConstraint(['detector_name'], ['outlier_detector.name'], ),
    sa.PrimaryKeyConstraint('astro_object', 'detector_name', 'score')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('outlier_score')
    op.drop_table('outlier_detector')
    # ### end Alembic commands ###

from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column


class ModelBase(DeclarativeBase, MappedAsDataclass):
    pass


class DemoTable(ModelBase):
    __tablename__ = "demo"

    id: Mapped[int] = mapped_column(primary_key=True)
    data: Mapped[str]

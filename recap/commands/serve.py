import typer
from datetime import datetime
from fastapi import Depends, FastAPI, HTTPException
from pathlib import PurePosixPath
from typing import List, Generator
from recap import catalogs
from recap.catalogs.abstract import AbstractCatalog
from recap.config import settings
from recap.logging import setup_logging
from recap.types import Metadata


DEFAULT_URL = 'http://localhost:8000'

app = typer.Typer()
fastapp = FastAPI()
print(Metadata.schema_json(indent=2))

def get_catalog() -> Generator[AbstractCatalog, None, None]:
    with catalogs.open(**settings('catalog', {})) as c:
        yield c


# WARN This must go before get_path since get_path is a catch-all.
@fastapp.get("/search", response_model=List[Metadata])
def query_search(
    query: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> List['Metadata']:
    return [Metadata(**row) for row in catalog.search(query, as_of)]


@fastapp.get("/directory/{path:path}", response_model=List[str])
def list_directory(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> List[str]:
    children = catalog.ls(PurePosixPath(path), as_of)
    if children:
        return children
    raise HTTPException(
        status_code=404,
        detail=f"Directory not found at {path}"
    )


@fastapp.put("/directory/{path:path}")
def write_directory(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    return catalog.touch(PurePosixPath(path))


@fastapp.delete("/directory/{path:path}")
def delete_directory(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(PurePosixPath(path))


@fastapp.get("/metadata/{path:path}", response_model=Metadata)
def read_metadata(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    as_of: datetime | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> 'Metadata':
    # TODO should probably return a 404 if we get None from storage
    metadata = catalog.read(PurePosixPath(path), as_of)
    if metadata:
        return Metadata(**metadata)
    raise HTTPException(
        status_code=404,
        detail=f"Metadata not found at {path}"
    )


# TODO should dynamically add these for every type
@fastapp.put("/metadata/{path:path}")
def write_metadata(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    metadata: 'Metadata',
    catalog: AbstractCatalog = Depends(get_catalog),
):
    for type, type_metadata in dict(metadata).items():
        catalog.write(PurePosixPath(path), type, type_metadata)


@fastapp.delete("/metadata/{path:path}/{type}")
def delete_metadata(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    type: str | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(PurePosixPath(path), type)


@app.command()
def serve():
    """
    Starts a FastAPI server that exposes a catalog API over HTTP/JSON.
    """

    import uvicorn

    uvicorn.run(
        fastapp,
        log_config=setup_logging(),
        **settings('api', {}),
    )

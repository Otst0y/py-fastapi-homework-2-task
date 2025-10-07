from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from starlette import status

from database import get_db, MovieModel
from database.models import CountryModel, GenreModel, ActorModel, LanguageModel
from schemas import MovieDetailSchema, MovieListResponseSchema
from schemas.movies import MovieCreateSchema, MovieUpdateSchema

import pycountry

router = APIRouter()


@router.get("/movies/", response_model=MovieListResponseSchema)
async def get_movies(
        request: Request,
        page: int = Query(1, ge=1),
        per_page: int = Query(10, ge=1, le=20),
        db: AsyncSession = Depends(get_db)
):
    total_items = await db.scalar(select(func.count(MovieModel.id)))
    if total_items == 0:
        raise HTTPException(status_code=404, detail="No movies found.")

    total_pages = (total_items + per_page - 1) // per_page
    if page > total_pages:
        raise HTTPException(status_code=404, detail="No movies found.")

    offset = (page - 1) * per_page

    result = await db.execute(
        select(MovieModel).order_by(desc(MovieModel.id)).offset(offset).limit(per_page)
    )
    movies = result.scalars().all()

    if not movies:
        raise HTTPException(status_code=404, detail="No movies found.")

    prev_page: Optional[str] = None
    next_page: Optional[str] = None

    if page > 1:
        prev_page = f"/theater/movies/?page={page - 1}&per_page={per_page}"
    if page < total_pages:
        next_page = f"/theater/movies/?page={page + 1}&per_page={per_page}"

    return {
        "movies": movies,
        "prev_page": prev_page,
        "next_page": next_page,
        "total_pages": total_pages,
        "total_items": total_items
    }


@router.get("/movies/{movie_id}/", response_model=MovieDetailSchema)
async def get_movie(movie_id: int, db: AsyncSession = Depends(get_db)):
    query = (
        select(MovieModel)
        .options(
            selectinload(MovieModel.country),
            selectinload(MovieModel.genres),
            selectinload(MovieModel.actors),
            selectinload(MovieModel.languages)
        )
        .where(MovieModel.id == movie_id)
    )

    movie = (await db.execute(query)).scalar_one_or_none()

    if not movie:
        raise HTTPException(status_code=404, detail="Movie with the given ID was not found.")
    return movie


@router.post("/movies/", response_model=MovieDetailSchema, status_code=201)
async def post_movie(movie: MovieCreateSchema, db: AsyncSession = Depends(get_db)):
    existing_movie = await db.scalar(select(MovieModel).where(
        MovieModel.name == movie.name,
        MovieModel.date == movie.date
    ))
    if existing_movie:
        raise HTTPException(
            status_code=409,
            detail=f"A movie with the name '{movie.name}' and release date '{movie.date}' already exists."
        )

    country = await db.scalar(select(CountryModel).where(CountryModel.code == movie.country))
    if not country:
        country_name = pycountry.countries.get(alpha_3=movie.country).name
        country = CountryModel(code=movie.country, name=country_name)
        db.add(country)
        await db.flush()

    async def get_or_create(model, name_list):
        items = []
        for name in name_list:
            obj = await db.scalar(select(model).where(model.name == name))
            if not obj:
                obj = model(name=name)
                db.add(obj)
                await db.flush()
            items.append(obj)
        return items

    genres = await get_or_create(GenreModel, movie.genres)
    actors = await get_or_create(ActorModel, movie.actors)
    languages = await get_or_create(LanguageModel, movie.languages)

    new_movie = MovieModel(
        name=movie.name,
        date=movie.date,
        score=movie.score,
        overview=movie.overview,
        status=movie.status,
        budget=movie.budget,
        revenue=movie.revenue,
        country=country,
        genres=genres,
        actors=actors,
        languages=languages
    )

    db.add(new_movie)
    await db.commit()
    await db.refresh(new_movie)

    movie_detail = await db.scalar(select(MovieModel).options(
        selectinload(MovieModel.country),
        selectinload(MovieModel.genres),
        selectinload(MovieModel.actors),
        selectinload(MovieModel.languages)
    ).where(MovieModel.id == new_movie.id))

    return movie_detail


@router.delete("/movies/{movie_id}/", status_code=status.HTTP_204_NO_CONTENT)
async def delete_movie(movie_id: int, db: AsyncSession = Depends(get_db)):
    query = (
        select(MovieModel)
        .options(
            selectinload(MovieModel.country),
            selectinload(MovieModel.genres),
            selectinload(MovieModel.actors),
            selectinload(MovieModel.languages)
        )
        .where(MovieModel.id == movie_id)
    )

    movie = (await db.execute(query)).scalar_one_or_none()

    if not movie:
        raise HTTPException(status_code=404, detail="Movie with the given ID was not found.")

    await db.delete(movie)
    await db.commit()


@router.patch("/movies/{movie_id}/", status_code=status.HTTP_200_OK)
async def update_movie(movie_id: int, movie_update: MovieUpdateSchema, db: AsyncSession = Depends(get_db)):
    query = (
        select(MovieModel)
        .options(
            selectinload(MovieModel.country),
            selectinload(MovieModel.genres),
            selectinload(MovieModel.actors),
            selectinload(MovieModel.languages)
        )
        .where(MovieModel.id == movie_id)
    )

    movie = (await db.execute(query)).scalar_one_or_none()

    if not movie:
        raise HTTPException(status_code=404, detail="Movie with the given ID was not found.")

    update_data = movie_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(movie, key, value)

    await db.commit()
    await db.refresh(movie)

    return {"detail": "Movie updated successfully."}

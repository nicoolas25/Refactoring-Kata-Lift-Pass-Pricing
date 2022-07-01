import datetime
import math
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI

from database import base_price as base_price_table
from database import database
from database import holidays as holidays_table
from database import insert, select

app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.put("/prices")
async def upsert_price(type: str, cost: int):
    await database.execute(
        insert(base_price_table)
        .values({"type": type, "cost": cost})
        .on_conflict_do_update(
            index_elements=['type'],
            set_={"cost": cost},
        )
    )

MAX_AGE = 200

DISCOUNT_BY_TYPE_BY_AGE = {
    "1jour": {
        range(0, 6): 100,
        range(6, 15): 30,
        range(15, 65): 0,
        range(65, MAX_AGE): 25,
    },
    "night": {
        range(0, 6): 100,
        range(6, 65): 0,
        range(65, MAX_AGE): 60,
    },
}

def apply_discount(price: float, *discount_percentages: int) -> float:
    import functools

    return functools.reduce(
        lambda total, discount_percentage: total * (1 - discount_percentage / 100),
        discount_percentages,
        price,
    )

def get_discount_for_age(type, age):
    return next(
        (
            discount
            for age_range, discount in DISCOUNT_BY_TYPE_BY_AGE[type].items()
            if age in age_range
        ),
        0
    )

def get_monday_discount(type, age, date, holidays) -> int:
    """Applies only out of holidays when you're older (>=) than 15 when it's not the night"""

    if (
        (date and date.weekday() == 0)  # We're a monday
        and (age and age >= 15)  # We're older than 15
        and type != 'night'  # We're not the night
        and (date not in [row.holiday for row in holidays])  # We're not a holidays
    ):
        return 35
    else:
        return 0


def cost(
    cost: int,
    holidays: List[Any],
    type: str,
    age: Optional[int],
    date: Optional[datetime.date],
) -> Union[float, int]:
    discount_for_age = get_discount_for_age(type, age)
    monday_discount = get_monday_discount(type, age, date, holidays)
    return apply_discount(cost, discount_for_age, monday_discount)


@app.get("/prices")
async def compute_price(
    type: str,
    age: Optional[int] = None,
    date: Optional[datetime.date] = None,
):
    result = await database.fetch_one(
        select(base_price_table.c.cost)
        .where(base_price_table.c.type == type),
    )

    holidays = await database.fetch_all(select(holidays_table))

    return {"cost": math.ceil(cost(result.cost, holidays, type, age, date))}

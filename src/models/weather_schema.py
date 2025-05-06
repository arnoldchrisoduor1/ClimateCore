from datetime import datetime
from typing import Dict, List, Optional, Union, Any
from pydantic import BaseModel, Field

class WeatherMain(BaseModel):
    temp: float
    feels_like: float
    temp_min: float
    temp_max: float
    pressure: int
    humidity: int
    sea_level: Optional[int] = None
    grnd_level: Optional[int] = None

class WeatherWind(BaseModel):
    speed: float
    deg: int
    gust: Optional[float] = None

class WeatherClouds(BaseModel):
    all: int

class WeatherSys(BaseModel):
    type: Optional[int] = None
    id: Optional[int] = None
    country: str
    sunrise: int
    sunset: int

class WeatherCoord(BaseModel):
    lon: float
    lat: float

class WeatherDescription(BaseModel):
    id: int
    main: str
    description: str
    icon: str

class WeatherData(BaseModel):
    id: int
    name: str
    coord: WeatherCoord
    weather: List[WeatherDescription]
    base: str
    main: WeatherMain
    visibility: int
    wind: WeatherWind
    clouds: WeatherClouds
    dt: int
    sys: WeatherSys
    timezone: int
    cod: int
    collection_time: str

class ProcessedWeatherData(BaseModel):
    city_id: int
    city_name: str
    country: str
    latitude: float
    longitude: float
    timestamp: datetime
    collection_time: datetime
    temperature: float
    temperature_feels_like: float
    temperature_min: float
    temperature_max: float
    pressure: int
    humidity: int
    weather_main: str
    weather_description: str
    wind_speed: float
    wind_direction: int
    cloudiness: int
    visibility: int
    sunrise: datetime
    sunset: datetime
    timezone_offset: int
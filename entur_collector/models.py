from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class Line(BaseModel):
    id: str
    name: str
    transportMode: str


class JourneyPattern(BaseModel):
    line: Line


class ServiceJourney(BaseModel):
    journeyPattern: JourneyPattern


class Quay(BaseModel):
    id: str


class DestinationDisplay(BaseModel):
    frontText: str


class EstimatedCall(BaseModel):
    realtime: bool
    aimedArrivalTime: datetime
    aimedDepartureTime: datetime
    expectedArrivalTime: datetime
    expectedDepartureTime: datetime
    quay: Quay
    serviceJourney: ServiceJourney


class StopPlace(BaseModel):
    id: str
    name: str
    estimatedCalls: List[EstimatedCall]


class Data(BaseModel):
    stopPlace: StopPlace


class Response(BaseModel):
    data: Data


class EnturData(BaseModel):
    response: Response
    timestamp: str
CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.earthquake
(
    time            DateTime64(3, 'UTC'),
    latitude        Nullable(Float64),
    longitude       Nullable(Float64),
    depth           Nullable(Float64),
    mag             Nullable(Float64),
    magType         Nullable(String),
    nst             Nullable(Int32),
    gap             Nullable(Float64),
    dmin            Nullable(Float64),
    rms             Nullable(Float64),
    net             Nullable(String),
    id              String,
    updated         Nullable(DateTime64(3, 'UTC')),
    place           Nullable(String),
    type            Nullable(String),
    horizontalError Nullable(Float64),
    depthError      Nullable(Float64),
    magError        Nullable(Float64),
    magNst          Nullable(Int32),
    status          Nullable(String),
    locationSource  Nullable(String),
    magSource       Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (time, id);

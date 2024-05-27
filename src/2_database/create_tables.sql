CREATE TABLE IF NOT EXISTS Clientes (
ID text,
Cod text,
CodSec text,
Sec text,
claseEconomica text,
Cantvisitas numeric,
IDDireccion text,
CodVision text
);


CREATE TABLE IF NOT EXISTS Territorio_1 (
IDDireccion2 text,
MunicipioDireccion text,
LongitudDireccion numeric,
LatitudDireccion numeric,
IDTerritorio text
);


CREATE TABLE IF NOT EXISTS Territorio_2 (
IDTerritorio text,
ComunaTerritorio text,
Territorio text,
SubregionTerritorio text
);


CREATE TABLE IF NOT EXISTS Venta (
fecha_fact timestamp,
fact text,
fact_pos text,
canal text,
ID text,
denominacion_1 text,
valor numeric,
servicio text,
descservicio text,
tiposervicio text,
tipopres text
);

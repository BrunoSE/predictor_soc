import MySQLdb
import pandas as pd
import logging
import ftplib
import os
from datetime import timedelta
from geopy import distance
from time import sleep
from sys import platform

global logger
global file_format

if platform.startswith('win'):
    ip_bd_edu = "26.2.206.141"
else:
    ip_bd_edu = "192.168.11.150"


def mantener_log():
    global logger
    global file_format
    logger = logging.getLogger(__name__)  # P: número de proceso, L: número de línea
    logger.setLevel(logging.DEBUG)  # deja pasar todos desde debug hasta critical
    print_handler = logging.StreamHandler()
    print_format = logging.Formatter('[{asctime:s}] {levelname:s} L{lineno:d}| {message:s}',
                                     '%Y-%m-%d %H:%M:%S', style='{')
    file_format = logging.Formatter('[{asctime:s}] {processName:s} P{process:d}@{name:s} ' +
                                    '${levelname:s} L{lineno:d}| {message:s}',
                                    '%Y-%m-%d %H:%M:%S', style='{')
    # printear desde debug hasta critical:
    print_handler.setLevel(logging.DEBUG)
    print_handler.setFormatter(print_format)
    logger.addHandler(print_handler)
    return logger


def procesar_datos_consulta(cursor):
    datos = [row for row in cursor.fetchall() if row[0] is not None]
    df_ = pd.DataFrame(datos, columns=[i[0] for i in cursor.description])
    df_.set_index('id', inplace=True)
    for columna in ['latitud', 'longitud', 'valor_soc', 'valor_ptg', 'valor_ptc']:
        try:
            df_[columna] = pd.to_numeric(df_[columna])
        except ValueError:
            logger.exception(f'Error en columna {columna}')

    df_['fecha_hora_consulta'] = pd.to_datetime(df_['fecha_hora_consulta'], errors='raise',
                                                format="%Y-%m-%d %H:%M:%S")
    df_['fecha_evento'] = pd.to_datetime(df_['fecha_evento'], errors='raise',
                                         format="%Y-%m-%d")
    df_['fecha_hora_evento'] = df_['fecha_evento'] + df_['hora_evento']

    return df_


def consultar_soc_ttec(fecha_dia):
    db1 = MySQLdb.connect(host=ip_bd_edu,
                          user="brunom",
                          passwd="Manzana",
                          db="tracktec")

    cur1 = db1.cursor()

    cur1.execute(
                 f"""
                 SELECT * FROM tracktec.eventos as te1 JOIN 
                 (SELECT evento_id as evento_id_soc, nombre as nombre_soc, 
                 valor as valor_soc FROM tracktec.telemetria_ 
                 WHERE (nombre = 'SOC' and 
                        valor REGEXP '^[\\-]?[0-9]+\\.?[0-9]*$')) as t_soc 
                 ON te1.id=t_soc.evento_id_soc 
                 WHERE fecha_evento = '{fecha_dia}'
                 AND hora_evento IS NOT NULL AND bus_tipo = 'Electric' 
                 AND PATENTE IS NOT NULL AND NOT (patente REGEXP '^[0-9]+')
                 ORDER BY patente;
                 """
                 )

    df__ = procesar_datos_consulta(cur1)

    cur1.close()
    db1.close()

    return df__


def descargar_data_ttec(fecha__):
    fecha__2 = fecha__.replace('-', '_')
    # logger.info(f"{consultar_soc_id(142339596)}")
    # df = consultar_transmisiones_con_soc_por_semana('2020-08-20', '2020-08-20')
    dfx = consultar_soc_ttec(fecha__)
    dfx.to_parquet(f'data_{fecha__2}.parquet', compression='gzip')


def descargar_semana_ttec(fechas):
    for fecha_ in fechas:
        logger.info(f"Descargando data Tracktec para fecha {fecha_}")
        descargar_data_ttec(fecha_)


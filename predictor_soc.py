import modulo_soc_por_expedicion as spe
from sys import platform
import pandas as pd
import os
global logger
global file_format

if platform.startswith('win'):
    ip_bd_edu = "26.2.206.141"
else:
    ip_bd_edu = "192.168.11.150"


def cruzar_gps_ttec(fecha):
    servicios_de_interes = ['F41', 'F46', 'F48', 'F63c', 'F67e', 'F83c',
                            'F69', 'F73', 'F81']
    df196r = pd.read_excel(f'Cruce_196resumen_data_{fecha}_revisado.xlsx')
    logger.info(f"Expediciones iniciales en resumen diario: {len(df196r.index)}")
    df196r = df196r.loc[df196r['Servicio'].isin(servicios_de_interes)]
    df196r = df196r.loc[~(df196r['hora_inicio'].isna())]


def p_pipeline(diap, mesp, annop, rd, rr, soc=True):
    fechas_de_interes, nombre_semana = spe.pipeline(diap, mesp, annop, rd, rr, solosoc=soc)

    df_f = []
    for fi in fechas_de_interes:
        logger.info(f'Cruzando data de fecha {fi}')
        df_f.append(cruzar_gps_ttec(fi))

    df_f = pd.concat(df_f)
    df_f['Intervalo'] = pd.to_datetime(df_f['Intervalo'], errors='raise',
                                       format="%H:%M:%S")

    df_f.to_parquet(f'dataf_{nombre_semana}.parquet', compression='gzip')
    logger.info('Listo todo para esta semana')

    os.chdir('..')


if __name__ == '__main__':
    logger = spe.mantener_log()
    reemplazar_data_ttec = False
    reemplazar_resumen = False
    p_pipeline(7, 9, 2020, reemplazar_data_ttec, reemplazar_resumen)
    logger.info('Listo todo')

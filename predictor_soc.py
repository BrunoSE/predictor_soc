import modulo_soc_por_expedicion as spe
from sys import platform
global logger
global file_format

if platform.startswith('win'):
    ip_bd_edu = "26.2.206.141"
else:
    ip_bd_edu = "192.168.11.150"


def p_pipeline(diap, mesp, annop, rd, rr, soc=True):
    fechas_de_interes = spe.pipeline(diap, mesp, annop, rd, rr, solosoc=soc)

    df_f = []
    for fi in fechas_de_interes:
        logger.info(f'Concatenando y mezclando data de fecha {fi}')
        df_f.append(mezclar_data(fi))

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

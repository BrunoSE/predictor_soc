import modulo_soc_por_expedicion as spe
from sys import platform
global logger
global file_format

if platform.startswith('win'):
    ip_bd_edu = "26.2.206.141"
else:
    ip_bd_edu = "192.168.11.150"


def descargar_data_ttec_soc(fecha__):
    fecha__2 = fecha__.replace('-', '_')
    # logger.info(f"{consultar_soc_id(142339596)}")
    # df = consultar_transmisiones_con_soc_por_semana('2020-08-20', '2020-08-20')
    dfx = spe.consultar_soc_ttec(fecha__)
    dfx.to_parquet(f'data_{fecha__2}.parquet', compression='gzip')


def pipeline_predictor():
    return

if __name__ == '__main__':
    logger = spe.mantener_log()
    spe.consultar_numero_transmisiones_por_semana()
    logger.info('Listo todo')

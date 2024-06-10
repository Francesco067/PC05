import pandas as pd
from pymongo import MongoClient
import requests

def limpiar_datos(df):
    print("Columnas originales:", df.columns.tolist())

    # Convertir nombres de columnas a cadenas y realizar las transformaciones
    df.columns = [str(col).strip().replace(' ', '_').replace('á', 'a').replace('é', 'e')
                  .replace('í', 'i').replace('ó', 'o').replace('ú', 'u').lower() for col in df.columns]
    print("Columnas después de convertir nombres:", df.columns.tolist())
    
    # Eliminar columnas duplicadas
    df = df.loc[:, ~df.columns.duplicated()]
    print("Columnas después de eliminar duplicadas:", df.columns.tolist())
       
    # Eliminar el carácter ',' de la columna DISPOSITIVO LEGAL
    if 'dispositivo_legal' in df.columns:
        df['dispositivo_legal'] = df['dispositivo_legal'].str.replace(',', '')
    
    return df

def obtener_datos_sunat():
    # Ejemplo de llamada a la API de SUNAT
    url = 'https://api.apis.net.pe/v1/tipo-cambio-sunat'
    response = requests.get(url)
    data = response.json()

    # Verificar y estructurar los datos correctamente
    if isinstance(data, dict):
        if all(isinstance(v, list) for v in data.values()):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame([data])
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        raise ValueError

    return df



def obtener_tipo_cambio():
    url = 'https://api.apis.net.pe/v1/tipo-cambio-sunat'
    response = requests.get(url)
    data = response.json()
    tipo_cambio = data['venta'] 
    return tipo_cambio

def dolarizar_montos(df, tipo_cambio):
    if 'monto_inversion' in df.columns:
        df['monto_inversion_usd'] = df['monto_inversion'] / tipo_cambio
    if 'monto_transferencia_2020' in df.columns:
        df['monto_transferencia_usd'] = df['monto_transferencia_2020'] / tipo_cambio
    return df

def transformar_estado(df):
    estado_map = {
        'Actos Previos': 1,
        'Concluido': 3,
        'Resuelto': 0,
        'En Ejecución': 2
    }
    if 'estado__ssp' in df.columns: 
        df['estado_puntuado'] = df['estado__ssp'].map(estado_map)
        df['estado__ssp'] = df['estado__ssp'].replace({
            0: 'Resuelto',
            1: 'Actos Previos',
            2: 'En Ejecución',
            3: 'Concluido'
        })
    else:
        
        df['estado_puntuado'] = " "
    return df

def manejar_columnas_vacias(df):
    columnas_necesarias = ['compra', 'venta', 'origen', 'moneda', 'fecha']
    for col in columnas_necesarias:
        if col not in df.columns:
            df[col] = None
    return df

# Leer el archivo Excel y especificar la fila de encabezado
df = pd.read_excel('./data/reactiva.xlsx', header=1)

# Limpiar los datos
df = limpiar_datos(df)

# Manejar columnas vacías
df = manejar_columnas_vacias(df)

# Obtener datos de la API de SUNAT y agregar al DataFrame
df_sunat = obtener_datos_sunat()
tipo_cambio = obtener_tipo_cambio()
df = dolarizar_montos(df,tipo_cambio)
df = pd.concat([df, df_sunat], ignore_index=True)


# Obtener el tipo de cambio actual
tipo_cambio = obtener_tipo_cambio()

# Dolarizar montos
df = dolarizar_montos(df, tipo_cambio)


# Transformar columna "Estado"
df = transformar_estado(df)


# Guardar el DataFrame procesado en un nuevo archivo Excel 
df.to_excel('./data/reactiva_procesado.xlsx', index=False)

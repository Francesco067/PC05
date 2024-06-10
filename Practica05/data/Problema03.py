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
    # Ejemplo de llamada a la API de SUNAT (deberás ajustar esto a tu caso real)
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
        raise ValueError("Los datos obtenidos de la API no están en un formato compatible.")

    return df



def obtener_tipo_cambio():
    url = 'https://api.apis.net.pe/v1/tipo-cambio-sunat'
    response = requests.get(url)
    data = response.json()
    tipo_cambio = data['venta']  # Usar el valor de venta
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
    if 'estado__ssp' in df.columns:  # Corregido el nombre de la columna
        df['estado_puntuado'] = df['estado__ssp'].map(estado_map)
        df['estado__ssp'] = df['estado__ssp'].replace({
            0: 'Resuelto',
            1: 'Actos Previos',
            2: 'En Ejecución',
            3: 'Concluido'
        })
    else:
        # Agregar columna estado_puntuado con valores predeterminados si no existe
        df['estado_puntuado'] = "A"
    return df

def manejar_columnas_vacias(df):
    # Agregar columnas vacías con valores predeterminados si no existen
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

# Ver columnas después de la limpieza y manejo de columnas vacías
print("Columnas después de la limpieza y manejo de columnas vacías:", df.columns.tolist())



# Obtener datos de la API de SUNAT y agregar al DataFrame
df_sunat = obtener_datos_sunat()
tipo_cambio = obtener_tipo_cambio()
df = dolarizar_montos(df,tipo_cambio)
df = pd.concat([df, df_sunat], ignore_index=True)

# Ver columnas después de agregar datos de SUNAT
print("Columnas después de agregar datos de SUNAT:", df.columns.tolist())



# Obtener el tipo de cambio actual
tipo_cambio = obtener_tipo_cambio()

# Dolarizar montos
df = dolarizar_montos(df, tipo_cambio)

# Ver columnas después de dolarizar montos
print("Columnas después de dolarizar montos:", df.columns.tolist())

# Transformar columna "Estado"
df = transformar_estado(df)

# Ver columnas después de transformar estado
print("Columnas después de transformar estado:", df.columns.tolist())

# Guardar el DataFrame procesado en un nuevo archivo Excel en el directorio de usuario
df.to_excel('./data/reactiva_procesado.xlsx', index=False)
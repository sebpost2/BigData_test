import streamlit as st
import pandas as pd
from hdfs import InsecureClient

# Configuración del cliente HDFS
hdfs_client = InsecureClient('http://Masterv2:50070', user='ec2-user')

# Diccionario para nombres de columnas personalizados para cada ruta
column_names = {
    "/sparkv2/Slave1v2/Genres": ['GenreName', 'Popularity'],
    "/sparkv2/Slave1v2/Locations": ['LocationName', 'Coordinates'],
    "/sparkv2/Slave1v2/Users": ['City', 'GenreName','Username','Count']
}

# Función para leer archivos CSV desde una ruta específica en HDFS
def read_csv_from_hdfs(hdfs_client, hdfs_path):
    try:
        csv_files = hdfs_client.list(hdfs_path)
        latest_csv_file = None
        for csv_file in csv_files:
            if csv_file.endswith('.csv') and csv_file.startswith('part'):
                latest_csv_file = csv_file

        if latest_csv_file:
            st.write(f"Mostrando datos de: {latest_csv_file}")

            with hdfs_client.read(f"{hdfs_path}/{latest_csv_file}") as reader:
                df = pd.read_csv(reader, header=None)

                # Asignar nombres de columnas personalizados según la ruta
                if hdfs_path in column_names:
                    df.columns = column_names[hdfs_path]

                st.dataframe(df)
                st.line_chart(df.set_index(df.columns[0])[df.columns[1]])  # Visualización personalizada
        else:
            st.write(f"No se encontraron archivos CSV válidos en: {hdfs_path}")
    except Exception as e:
        st.error(f"Error al acceder a HDFS en la ruta {hdfs_path}: {str(e)}")

# Título de la aplicación
st.title("Visualización de Datos desde HDFS")

# Rutas en HDFS donde se guardan tus archivos CSV
hdfs_paths = [
    "/sparkv2/Slave1v2/Genres",
    "/sparkv2/Slave1v2/Locations",
    "/sparkv2/Slave1v2/Users"
]

# Leer y mostrar los archivos CSV desde cada ruta en HDFS
for hdfs_path in hdfs_paths:
    st.header(f"Archivos en: {hdfs_path}")
    read_csv_from_hdfs(hdfs_client, hdfs_path)

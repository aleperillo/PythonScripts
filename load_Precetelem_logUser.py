import pymongo
import pyodbc
import logging
import datetime
from bson import ObjectId

# Configurar el sistema de registro
logging.basicConfig(filename='datatransfer.log', level=logging.INFO)

try:
   # Conexión a MongoDB
    mongo_client = pymongo.MongoClient("mongodb://user:password@serverIP:27017")
    mongo_db = mongo_client['Database']
    mongo_collection = mongo_db['Collection']

    # Conexión a SQL Server
    server_ip = 'SqlServer'
    database = 'SqlDatabasename'
    user = 'user_mongo'
    password = ''


    sql_server_connection_string = (
    f'DRIVER=FreeTDS;SERVER={server_ip};PORT=1433;DATABASE={database};UID={user};PWD={password}'
    )
    sql_server_connection = pyodbc.connect(sql_server_connection_string)
    sql_server_cursor = sql_server_connection.cursor()

    # Truncate en la logUser
    sql_server_cursor.execute('TRUNCATE TABLE Precetelem.logUser')
    sql_server_connection.commit()

    # Obtener datos de MongoDB y cargar en SQL Server
    for document in mongo_collection.find():
        _id = document.get('_id', '')
        idUsuario = document.get('idUsuario', '')
        ciudad_localizacion = document.get('ciudad_localizacion', '')
        fechaServidor = document.get('fechaServidor', '')
        horaServidor = document.get('horaServidor', '')
        fechaLocal = document.get('fechaLocal', '')
        horaLocal = document.get('horaLocal', '')
        direccion_ip = document.get('direccion_ip', '')
        pagina = document.get('pagina', '')
        control = document.get('control', '')
        extra = document.get('extra', '')

        # Mapeo de campos entre MongoDB y SQL Server
        _id_str = str(_id)

        # Inserción en SQL Server dentro del bucle
        sql_query = """
            INSERT INTO Precetelem.logUser (_id, ciudad_localizacion, control1, direccion_ip, extra, fechaLocal, fechaServidor, horaLocal, horaServidor, idUsuario, pagina)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        sql_server_cursor.execute(
            sql_query,
            _id_str, ciudad_localizacion, control, direccion_ip, extra,
            fechaLocal, fechaServidor, horaLocal, horaServidor, idUsuario, pagina
        )
        sql_server_connection.commit()
 # Cerrar conexiones
    sql_server_cursor.close()

    logging.info(f'Ejecución del archivo load_Precetelem_logUser.py de forma exitosa: {datetime.datetime.now()}')
except Exception as e:
    # Manejar errores y registrar mensajes de error
    logging.error(f'Error durante la ejecución del archivo load_Cetelem_logUser.py: {str(e)}')
		

import pymongo
import pyodbc
from bson import ObjectId

# Conexión a MongoDB
mongo_client = pymongo.MongoClient("mongodb://dba_mok:4R1o0cd*oZ3*@10.80.24.95:27017")
mongo_db = mongo_client['logUser_Precetelem']
mongo_collection = mongo_db['logUser']

# Conexión a SQL Server
server_ip = '10.80.24.165'
database = 'DW_Ecosistemas_PT'
user = 'user_mongo'
password = ''

sql_server_connection_string = (
    f'DRIVER=FreeTDS;SERVER={server_ip};PORT=1433;DATABASE={database};UID={user};PWD={password}'
)
sql_server_connection = pyodbc.connect(sql_server_connection_string)
sql_server_cursor = sql_server_connection.cursor()

# Truncate en la logUser
sql_server_cursor.execute('TRUNCATE TABLE logUser')
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
        INSERT INTO logUser (_id, ciudad_localizacion, control1, direccion_ip, extra, fechaLocal, fechaServidor, horaLocal, horaServidor, idUsuario, pagina)
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
sql_server_connection.close()

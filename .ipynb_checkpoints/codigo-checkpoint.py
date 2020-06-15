
# coding: utf-8

# In[1]:


#cuando no se tiene instalado una libreria en python se usa el comando pip pero para hacer la instalacion global se puede hacer con el comando !pip 
get_ipython().system('pip install cassandra-driver')


# In[2]:


#importamos esa libreria su clase cluster 
from cassandra.cluster import Cluster


# In[3]:


#conectamos nuestro codigo python con la cassandra que esta corriendo en el contenedor docker 
#recordemos que podemos conectarnos a mas nodos delimitando distintas ips por ','	
cluster = Cluster(contact_points=['192.168.0.16'], port = 9042)
session = cluster.connect()


# ### Leemos el fichero con pandas

# In[5]:


#importamos y renombramos por cuestiones de buenas practicas 
import pandas as pd
import numpy as np


# In[7]:


#leemos nuestro fichero csv 
#especificamos la ruta del fichero (para saber nuestra ruta podemos ejecutar el comando !ls similar a linux) 	
suicides = pd.read_csv('./suicidios.csv', sep = ',', thousands=b',', header=0)


# In[8]:


#tenemos nuestro dataframe 	
suicides.head()


# In[9]:


#podemos ver que hay unas columnas algo extrañas	
suicides.columns


# ### Renombramos las columnas

# In[10]:


suicides.rename({'suicides/100k pop':'suicides_by_100k_pob', 
                 'HDI for year':'HDI_for_year',
                 ' gdp_for_year ($) ': 'pib_for_year',
                 'gdp_per_capita ($)': 'pib_per_capita'}, axis=1, inplace=True)


# In[11]:


#el implace es un reemplazo definitivo mas no una copia 	
suicides.head()


# In[12]:


suicides.columns


# In[13]:


#¿que tipos de datos son nuestras columnas?
suicides.dtypes


# In[14]:


#tenemos una descripcion general de nuestra tabla con caracteristicas como el conteo, la media, la desviacion estandar, el maximo y el minimo etc...
suicides.describe()


# In[15]:


#vamos a definir nuestros valores vacios (NaN), el .sum() hace una sumatoria de estos valores vacios 	
print(f"Existen {np.isnan(suicides.year).sum()} valores nan para la variable 'year'")
print(f"Existen {np.isnan(suicides.suicides_no).sum()} valores nan para la variable 'suicides_no'")
print(f"Existen {np.isnan(suicides.population).sum()} valores nan para la variable 'population'")
print(f"Existen {np.isnan(suicides.suicides_by_100k_pob).sum()} valores nan para la variable 'suicides_by_100k_pob'")
print(f"Existen {np.isnan(suicides.HDI_for_year).sum()} valores nan para la variable 'HDI_for_year'")
print(f"Existen {np.isnan(suicides.pib_per_capita).sum()} valores nan para la variable 'pib_per_capita'")


# ### Vamos a eliminar la variable 'HDI_for_year'

# In[16]:


#eliminamos esta variable axis 1 para nivel de columna y implace para eliminar directamente y no copiar o replicar 
suicides.drop('HDI_for_year', axis=1, inplace=True)


# In[17]:


suicides.columns


# ## Creamos nuestro modelo de datos, comenzamos creando el keyspace y la tabla

# In[18]:


session.execute(
    """CREATE KEYSPACE IF NOT EXISTS PROYECTO WITH replication = {'class': 'SimpleStrategy', 
                                                    'replication_factor' : 1}; """
)


# In[19]:


#usamos nuestro keyspace es decir que tenemos nuestra variable session apuntando a ese keyspace	
session.set_keyspace("proyecto")


# In[21]:


#creamos una clase en python para crear nuestro modelo de datos cassandra
#este procedimiento es opcional porque podemos crear nuestro modelo de datos de manera tradicional con nuestra variable session 
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import models
from cassandra.cqlengine import connection

class Suicide(Model):
    country = columns.Text(primary_key = True)
    year = columns.Integer(primary_key = True)
    sex  = columns.Text(primary_key = True)
    age = columns.Text(primary_key = True, clustering_order="ASC")
    suicides_no = columns.Integer()
    population = columns.BigInt()
    suicides_by_100k_pob = columns.Float()
    country_year = columns.Text()
    pib_for_year = columns.BigInt()
    pib_per_capita = columns.BigInt()
    generation = columns.Text()


# In[22]:


#esta libreria sincroniza la base de datos y crea la base de datos 	
from cassandra.cqlengine.management import sync_table
#debemos registrar nuestra conexion le damos un nombre y le asignamos nuestra variable sesion 
connection.register_connection('cluster3', session=session)
sync_table(Suicide, keyspaces=['proyecto'],connections=['cluster3'])


# In[23]:


#advertencia de futuras versiones 

#verificamos que nuestra tabla fue generada exitosamente 	
for row in session.execute("""select column_name, kind, type from system_schema.columns WHERE keyspace_name = 'proyecto' ;"""):
    print(row)


# ### Vamos a insertar los datos de suicidio en la tabla

# In[24]:


#vamos a 'volcar' los datos de nuestro dataframe en la tabla cassandra que acabamos de crear, esto lo haremos de manera secuencial fila a fila 
#por medio de un bucle for
#la variable index viene por defecto en el dataframe pero esta no se va a agregar en la base de datos 
#la '%s' equivale a los values fuera de la sentencia cql
#podemos limitar la cantidad de tuplas o registros que queremos pasar a nuestra base de datos con el metodo del dataframe suicides.head(n)
for index,value in suicides.iterrows():
    session.execute(""" INSERT INTO proyecto.suicide (country, year, sex, age, country_year, generation,  
                            pib_for_year, pib_per_capita, population,  suicides_by_100k_pob, 
                            suicides_no)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                   (value['country'], value['year'], value['sex'], value['age'], 
                   value['country-year'], value['generation'], value['pib_for_year'], value['pib_per_capita'], 
                   value['population'], value['suicides_by_100k_pob'], value['suicides_no']) )


# In[25]:


#podemos saber que tipo de variable es nuestra variable rows con el comando type(my_rows)	
all_rows = session.execute("""SELECT * FROM proyecto.suicide""")

for row in all_rows[:20]:
    print(row)


# In[26]:


for row in all_rows[0:20]:
    print(f"""En {row['country']} en el año {row['year']} hubo {row['suicides_no']} suicidios de genero {row['sex']} con edades comprendidas en {row['age']} """)


# ### También podemos convertirlo en dataframe

# In[27]:


df = pd.DataFrame()
for row in all_rows[:20]:
    df = df.append(row, ignore_index=True)


# In[28]:


#vamos hacia adelante y hacia atras XD	
df = pd.DataFrame()
for row in all_rows[:20]:
    df = df.append(row, ignore_index=True)


# In[29]:


df.head()


# In[30]:


df.dtypes


# ## ¿De cuántos paises tenemos datos en el fichero?

# In[31]:


#lanzo una sentencia cql la cual me devuelva unicamente la columna pais, eso lo pasamos a un dataframe de una columna 
#recorro con un bucle for 
#es de recordar que para saber el tamaño de nuestra tabla podemos hacerlo con el metodo .shape
num_paises = session.execute(""" SELECT country FROM suicide """)

df_num_paises = pd.DataFrame()
for row in num_paises:
    df_num_paises = df_num_paises.append(row, ignore_index = True)


# In[32]:


df_num_paises.head()


# In[33]:


#el metodo .unique() devuelve valores unicos 	
#con el metodo len () nos devuelve el tamaño exacto
print(f"Existen datos de {len(df_num_paises.country.unique())} paises")


# ## ¿Como Recuperar aquellos registros con más de 250 suicidios en un año?

# In[34]:


#asi no es
suicide_greater_250 = session.execute(""" SELECT * FROM suicide WHERE suicides_no > 250""")


# In[35]:


#de este modo no tendriamos problema 	
suicide_greater_250 = session.execute(""" SELECT * FROM suicide WHERE suicides_no > 250 ALLOW FILTERING""")
df_suicide_greater_250 = pd.DataFrame()
for row in suicide_greater_250:
    df_suicide_greater_250 = df_suicide_greater_250.append(row, ignore_index = True)


# In[36]:


#aca los listamos 
#podemos saber el minimo con el metodo .min() o el .max()
df_suicide_greater_250.head()


# In[37]:


#saber cuantas tuplas hay con mas de 250 suicidios 	
df_suicide_greater_250.shape


# ### ¿Como Recuperar los registros filtrando por genero. Si no es posible, crear un indice secundario.?

# In[38]:


#no funciona
female_suicides = session.execute(""" SELECT * FROM suicide WHERE sex = 'female'""")


# In[39]:


#creamos un indice secundario 
session.execute(""" CREATE INDEX IF NOT EXISTS sex_suicide ON suicide (sex)""")


# In[40]:


#con nuestro indice secundario ya podemos filtrar por campos no pertenecientes a la primary key 	
female_suicides = session.execute(""" SELECT * FROM suicide WHERE sex = 'female'""")


# In[41]:


df_female_suicides = pd.DataFrame()
for row in female_suicides:
    df_female_suicides = df_female_suicides.append(row, ignore_index = True)


# In[42]:


df_female_suicides.head()


# #### ¿Cómo crear una vista materializada para poder filtrar por el campo generation y conteniendo unicamente los registros de paises con poblacion mayor a 10 millones de personas?
# 

# In[43]:


#creamos nuestra vista materializada es decir nuestra tabla proveniente de una consulta 
#las vistas materializadas no pueden tener nulos en las primary keys 
#le damos permiso a nuestra variable generacion para que se pueda filtrar, el resto de variables ya son primary key 
session.execute("""
    CREATE MATERIALIZED VIEW suicides_by_generation AS
        SELECT * FROM proyecto.suicide
        WHERE generation is not null and country is not null and 
        year is not null and age is not null and sex is not null and
        population > 10000000
        PRIMARY KEY (generation, country, year, age, sex)
    WITH comment='Vista materializada de generation';
""")


# In[44]:


#consultamos nuestra vista materializada como si fuera una tabla mas 
generation_x = session.execute("""SELECT * FROM suicides_by_generation WHERE generation = 'Generation X'""")


# In[45]:


df_generation_x = pd.DataFrame()
for row in generation_x:
    df_generation_x = df_generation_x.append(row, ignore_index = True)


# In[46]:


df_generation_x.head()


# In[48]:


df_generation_x.shape
#si se insertara un registro o tupla nuevo se va a ver reflejado en la vista materializada 


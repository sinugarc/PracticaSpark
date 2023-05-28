# Practica Spark
<sub> Sinhue García Gil,  Cristina Hernando Esperanza,  Daniel Martínez Martín </sub>

FALTA
- [ ] limpiar el codigo de prints

- [x] 1. Definición clara y precisa del problema a resolver.
- [x] 2. Diseño y la implementación en Spark de la solución al problema propuesto y explicaciones de funciones
- [ ] 3. Explicacion de los resultados y conclusion, motivación, ejemplos,  detalles importantes de la implementación, evaluación de resultados,


## Definición del problema a resolver
El problema que nos planteamos es analizar el destino preferido de una región de Madrid.

Dado una zona a estudiar, mediante el uso de los archivos de la base de datos de BICIMad, un porcentaje y un intervalo de tiempo de uso constante; queremos que nos devuelva una o varias zonas que corresponden al destino preferido.

Consideramos como destino preferido a dichas zonas que cumplan una de las dos siguientes propiedades según la opción dada como argumento:

* Opción 0:   Las zonas tienen que cubrir entre todas, manteniendo el orden de preferencia, el porcentaje dado.

* Opción 1:   Las zonas tiene que superar, cada una, el porcentaje dado.


> ### Ejemplo
> 
> Dada la zona A de origen, el total de viajes que salen de A son 100
>
>
> | Zonas     | 1  |  2 | 3  |  4 |
> | ----------| ---|----|----|----|
> | Nº Viajes | 40 | 30 | 20 | 10 |
>
> Dada la opción 0 y el porcentaje 20, solo nos devolvería la zona 1.
> 
> Dada la opción 0 y el porcentaje 50, nos devolvería la zona 1 y 2.
> 
> Dada la opción 1 y el porcentaje 20, devolvería la zona 1, 2 y 3. 
> 
> Dada la opción 1 y el porcentaje 50, no nos devolvería ninguna zona.



## Diseño e implementación en Spark 

Para poder clasificar las estaciones en distintas zonas, necesitamos el archivo infile2 *"202007.json"* (o su equivalente de otro año, siempre y cuando mantenga el mismo formato) ya que este contiene para cada estación, identificada con un número del 1 al 220, sus coordenadas geográficas. 

Además necesitamos para cada estación de origen que pertenezca a la zona dada a estudiar, sus estaciones de destino. Esta información viene dada en otro archivo distinto infile1 *"202007_movements.json"* (o su equivalente de otro año) que contiene todos los movimientos realizados durante el mes. 

Vamos a definir una zona en este contexto como una celda de la cuadrícula generada por todas las estaciones y un número dado, n (usualmente 5). Generamos esta cuadrícula tomando la longitud y latitud, máxima y mínima del total de estaciones y las tomamos como referencia para los laterales de la cuadrícula. Se crean intervalos equiespaciados tal que dividan esta cuadrícula en $n$ x $n$,  produciendo $n^2$ celdas ordenadas de la siguiente manera:

> |  $n^2$-n|$n^2$-n+1| ... | $n^2$-1 |
> | ----| ----|-----|------|
> |   ... | ... | ... |  ... |
> |    n  | n+1 | ... | 2n-1 |
> |    0  |  1  | ... |   n-1 |

### Funciones

#### getTpla()

Dado un diccionario que contiene la información de una sola estación, extraemos sólo el id, nombre y coordenadas.
```ruby
def getTpla(x):
  tpla = (x['id'],
          x['name'],
          float(x['latitude']),
          float(x['longitude']))
  return tpla
```

#### identifyZone()

Dada la tupla extraída previamente y unas constantes de longitud y latitud (máximo, mínimo y las longitudes de la celda), devuelve la zona a la que pertenece dicha estación con su identificación.

```ruby
def identifyZone(x,n,min_lat,cte_lat,min_long,cte_long):
    zona_lat=(x[2]-min_lat)//cte_lat
    zona_long=(x[3]-min_long)//cte_long
    zona = zona_lat*n + zona_long
    return (int(zona),x[0])
```

#### agruparZona()

Dado el fichero de entrada infile2, extraemos de su primera línea (todas son equivalentes para las columnas que nos interesan), la lista de diccionarios que contiene la información de cada estación. Aplicamos a cada estación la función *getTpla()* y con los valores calculados de latitud y longitud aplicamos la función *identifyZone()*.

Agrupamos por zonas las estaciones y devolvemos el conjunto de datos como un diccionario.

```ruby
def agruparZona(sc,filename,n):
    sc.setLogLevel("ERROR")
    data = sc.textFile(filename)
    mov = data.map(lambda x: json.loads(x)).take(1)[0]["stations"] 
    datos=sc.parallelize(mov)
    estaciones = datos.map(getTpla)
    
    est=estaciones.collect() #len(estaciones) <230
    
    max_long=max(est,key=(lambda x: x[3]))[3]
    min_long=min(est,key=(lambda x: x[3]))[3]
    max_lat=max(est,key=(lambda x: x[2]))[2]
    min_lat=min(est,key=(lambda x: x[2]))[2]
    
    cte_long=(max_long-min_long)/float(n)
    cte_lat=(max_lat-min_lat)/float(n)
    
    lista_zonas=estaciones.map(lambda x : identifyZone(x,n,min_lat,cte_lat,min_long,cte_long))\
                          .groupByKey().mapValues(lambda x : list(x))

    return lista_zonas.collectAsMap()
```

#### getTpla2()

Dado un diccionario que contiene la información de un solo viaje, extraemos sólo el id de las estaciones de origen y destino, y el tiempo de viaje.

 <sub> No confundir con la función getTpla, que pasa la informacion de una estacion (del archivo infile2) mientras que getTpla2 recibe la información de cada viaje (del archivo infile1) </sub> 

```ruby
def getTpla2(x):
  tpla = (x['idunplug_station'],
           x['idplug_station'],
           x['travel_time'])
  return tpla
```

#### elegir_preferido()

Según la lista de candidatos, ordenada de mayor a menor según los viajes por zona, el porcentaje a cubrir y la opción dada; selecciona los mejores candidatos. 
Devuelve el total de viajes que salen de la zona elegida y una lista que contiene las zonas preferidas y sus correspendientes viajes.

```ruby
def elegir_preferido(lista, perc, opcion):
    total=0
    for i in lista:
      total+=i[0]
    j=0
    L=[]
    if opcion==1:
      while lista[j][0] > perc*total:
          L.append((lista[j][1],lista[j][0]))
          j+=1
    else:
      acum2=0
      while acum2 < perc*total:
            acum2+=lista[j][0]
            L.append((lista[j][1],lista[j][0]))
            j+=1
  
    return total, L
```
#### cambiar_de_id_zona()

Dado la tupla obtenida de un viaje y un diccionario que contiene por cada zona las estaciones que pertenecen a él (obtenido con la función *agruparZona()* ) devuelve la zona a la que pertenece la estación de destino. 
También devuelve un 1 para facilitar el próximo uso de *groupByKey* .

```ruby
def cambiar_de_id_zona(ida, dict_lista_zonas):
    for key in dict_lista_zonas:
      if ida[1] in dict_lista_zonas[key]:
        return (key,1)
```

#### función principal()

Crea a partir del archivo infile1 un rdd; seleccionamos las columnas deseadas con la función *getTpla2* y filtramos aquellos viajes que se encuentren el intervalo de tiempo que queramos. 
De todos los viajes, elegimos sólo los que partan de estaciones que se encuentren en la zona a estudiar, y agrupamos según la zona de la estación de destino. 
Cambiamos el formato de los datos para poder ordenarlos según el número de viajes de cada zona. Aplicamos la función *elegir_preferido* para obtener las zonas preferidas y viajes correspondientes.
Por último escribimos en un fichero de salida los resultados obtenidos.

```ruby
def F(sc, zona_a_analizar, lista_zonas, infile1, outfile, perc, opcion):
    rdd_base = sc.textFile(infile1)
    bicis = rdd_base.map(lambda x: json.loads(x))
    movimientos = bicis.map(getTpla2)\
                  .filter(lambda x: x[2] >= 700 and x[2]<=1000 ) 
    id_zona= lista_zonas[zona_a_analizar]  
    rdd= movimientos.filter(lambda x: x[0] in id_zona)\
                    .map(lambda x: cambiar_de_id_zona(x,lista_zonas))\
                    .groupByKey()\
                    .mapValues(lambda x: len(x))\
                    .map(lambda x: (x[1],x[0]))\
                    .sortByKey(False)
    total, L=elegir_preferido(rdd.collect(), perc ,opcion)

    outf = open(outfile, "w")
    outf.write(f'El total de movimientos es {total} que salen de la zona {zona_a_analizar} \n')
    if opcion==1:
      outf.write(f'Las zonas preferidas con porcentaje mayor que {perc} son: \n')
    else:
      outf.write(f'Las zonas que suman al menos un porcentaje mayor que {perc} son: \n')
    for line in L:
        outf.write(str(line) + '\n')
    
    outf.close()
```

#### función a ejecutar 

Primero creamos la cuadrícula de zonas y asignamos a cada estación a una zona con la función *agruparZona* y se la pasamos como argumento a la función principal F.

Para ejecutar el programa necesitamos pasarle como argumentos: infile1, infile2, outfile, el número de filas de la cuadrícula, la zona a estudiar, el porcentaje que consideremos como preferido y la opción para decidir los preferidos.

```ruby
def main(infile1, infile2, outfile, zonaSetUp, zona, perc, opcion):
    sc=SparkContext()
    b= agruparZona(sc,infile2,zonaSetUp)
    F(sc,zona, b, infile1,outfile,perc, opcion)

if __name__ == '__main__':
    if len(sys.argv) != 8:
        print("Uso: python3 {0} <fileInMovements> <fileInStations> <fileOut> <#zoneSetUp> <targetZone> < % > < Option >".format(sys.argv[0]))
    else:
        p=float(sys.argv[6])
        if p>1:
            p=p/100.
        main(sys.argv[1],sys.argv[2],sys.argv[3],int(sys.argv[4]),int(sys.argv[5]),p,int(sys.argv[7]))
```


## Explicación de los resultados y conclusión, motivación, ejemplos,  detalles importantes de la implementación, evaluación de resultados.


Sinhue??? 

    

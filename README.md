# PracticaSpark

FALTA:
-AgruparZona: cuadricula
-usar hadoop(?)
-Decidir como añalizar: maximo,porcentaje
-Probar en python

Nuestra idea es dado dos ficheros del mismo mes, uno con la información de los movimientos de todo el mes (id de estación inicio, id de estación destino y tiempo de recorrido), y otro con la información de la situacion de cada estación (este contiene la ubicación de cada estación)

0.Agrupar zonas según id
def agruparZona(sc, infile2):
    rdd_base = sc.textFile(infile2)
    estaciones = rdd_base.map(lambda x: json.loads(x)).take(1) 
    estaciones = estaciones['estaciones'] [{..., long:40 lat:40  id:1}]
    
    
1. Elegir de cada movimiento solo los datos que necesitamos

def getTpla(x):
  tpla = (x['idunplug_station'],
           x['idplug_station'],
           x['travel_time'])
  return tpla
  
2.

def cambiar_de_id_zona(id, dict_lista_zonas):
    for key, value in dict_lista_zonas:
      if id in value:
        return key

def F(sc, zona_a_analizar, lista_zonas, infile1, ofile):
    rdd_base = sc.textFile(infile1)
    bicis = rdd_base.map(lambda x: json.loads(x))
    movimientos = bicis.map(getTpla)\
                  .filter(segun el 3 argumento, acotar respecto a ciertos segundos)  [ (idi,idf,t)]
    id_zona= lista_zonas[zona_a_analizar]
    rdd= movimientos.filter(lambda x: idi in id_zona)\  [(id,idf,t)] que nos interesan
                    .map(lambda x: cambiar_de_id_zona(x[1],lista_zonas)) [zonaf, zonaf, zonaf]
                    .groupByKey (si existe) {zonaf: cont, ...}
    #tomamos el maximo, el porcentaje ,...               
    
   
    
    outf = open(outfile, "w")
    for line in to_print:
        outf.write(line + '\n')
    outf.close()
    

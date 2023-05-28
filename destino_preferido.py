from pyspark import SparkContext
import json
import sys

def getTpla(x):
    tpla = (x['id'],
            x['name'],
            float(x['latitude']),
            float(x['longitude']))
    return tpla

def identifyZone(x,n,min_lat,cte_lat,min_long,cte_long):
    zona_lat=(x[2]-min_lat)//cte_lat
    zona_long=(x[3]-min_long)//cte_long
    zona = zona_lat*n + zona_long
    return (int(zona),x[0])

def agruparZona(sc,filename,n):
    data = sc.textFile(filename)
    mov = data.map(lambda x: json.loads(x)).take(1)[0]["stations"] 
    
    datos=sc.parallelize(mov)
    estaciones = datos.map(getTpla)
    
    max_long=estaciones.max(key=(lambda x: x[3]))[3]
    min_long=estaciones.min(key=(lambda x: x[3]))[3]
    max_lat=estaciones.max(key=(lambda x: x[2]))[2]
    min_lat=estaciones.min(key=(lambda x: x[2]))[2]
    
    cte_long=(max_long-min_long)/float(n)
    cte_lat=(max_lat-min_lat)/float(n)
    
    lista_zonas=estaciones.map(lambda x : identifyZone(x,n,min_lat,cte_lat,min_long,cte_long))\
                          .groupByKey().mapValues(lambda x : list(x))

    return lista_zonas.collectAsMap()
    
def getTpla2(x):
    tpla = (x['idunplug_station'],
            x['idplug_station'],
            x['travel_time'])
    return tpla
  
def elegir_preferido(lista, perc, opcion):
    total=0
    for i in lista:
        total+=i[0]
    j=0
    L=[]
    if opcion==1:
        while lista[j][0]>perc*total:
            L.append((lista[j][1],lista[j][0]))
            j+=1
    else:
        acum2=0
        while acum2<perc*total:
            acum2+=lista[j][0]
            L.append((lista[j][1],lista[j][0]))
            j+=1
      
    return total, L
    

def cambiar_de_id_zona(ida, dict_lista_zonas):
    for key in dict_lista_zonas:
        if ida[1] in dict_lista_zonas[key]:
            return (key,1)

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
    outf.write(f'Los movimientos registrados, que salen de la zona {zona_a_analizar}, son {total}. \nEstos provienen de las estaciones {lista_zonas[zona_a_analizar]} \n\n')
    
    if opcion==1:
      outf.write(f'Las zonas preferidas con porcentaje mayor que {perc*100}% son: \n\n')
      
    else:
      outf.write(f'Las zonas que en total suman un porcentaje de viajes mayor que {perc*100}% son: \n')
      
    for line in L:
        p=int(line[1]/total*10000)/100.
        outf.write(f'- Zona {line[0]} con {line[1]} viajes que acumula el {p}% de los viajes\nLas estaciones pertenecientes a esta zona son {lista_zonas[line[0]]} \n')
        
    if len(L)==0:
        outf.write('Ninguna zona cumple los requisitos solicitados')
    
    outf.close()


def main(infile1,infile2,outfile,zonaSetUp,zona,perc,opcion):
    sc=SparkContext()
    sc.setLogLevel("ERROR")
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
#Práctica de Spark

#librerias
pacman::p_load(httr, tidyverse,leaflet,janitor,readr,sparklyr, XML, xlsx )

#spark
library(sparklyr)
library(dplyr)
sc <- spark_connect(master = "local")


#código
url<-	"https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"

httr::GET(url)

#A. De fuentes veraces, lea los archivos que se indican en el anexo, como podrá apreciar, el/los archivos contiene miles de filas por decenas de columnas; solo es posible tratarlos utilizando Spark si queremos respuestas en tiempo real;

#i.Limpie el/los dataset(s) ( la información debe estar correctamente formateada, por ej. lo que es de tipo texto no debe tener otro tipo que no sea texto) , ponga el formato correcto en los números, etc., etc.
ds <- jsonlite::fromJSON(url)
ds <- ds$ListaEESSPrecio
ds <- ds %>% as_tibble() %>% clean_names() 

ds <- ds  %>% type_convert(locale = locale(decimal_mark = ",")) %>% view() %>% clean_names() 

view(ds)

#ii. genere un informe y explique si encuentra alguna anomalía, en el punto ii.

#ready en hadoop

#iii. cree una columna nueva que deberá llamarse low-cost, y determine cuál es el precio promedio de todos los combustibles a nivel comunidades autónomas, así como para las provincias, tanto para el territorio peninsular e insular, esta columna deberá clasificar las estaciones por lowcost y no lowcost,

ds<- ds %>%mutate(lowcost=rotulo%in%c("REPSOL","CAMPSA","BP", "SHELL","GALP", "CEPSA")) %>% view()

#hacer mutate/replace para low_cost y no low cost renombrar
ds$lowcost[ds$lowcost == TRUE] <- 'No_Lowcost'
ds$lowcost[ds$lowcost == FALSE] <- 'Lowcost'

View(ds)

media_total_comunidades <- ds %>% select(precio_bioetanol, precio_biodiesel, precio_gas_natural_comprimido,precio_gas_natural_licuado, precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, precio_gasolina_98_e5, precio_hidrogeno, rotulo, idccaa, provincia) %>% 
  group_by(idccaa) %>% summarise(media_precio_bioetanol=mean(precio_bioetanol, na.rm=TRUE), mean(precio_biodiesel, na.rm=TRUE), mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), mean(precio_hidrogeno, na.rm=TRUE)) %>% view()

media_total_provincias <-  ds %>% select(precio_bioetanol, precio_biodiesel, precio_gas_natural_comprimido,precio_gas_natural_licuado, precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, precio_gasolina_98_e5, precio_hidrogeno, rotulo, idccaa, provincia) %>% 
  group_by(provincia) %>% summarise(media_precio_bioetanol=mean(precio_bioetanol, na.rm=TRUE), mean(precio_biodiesel, na.rm=TRUE), mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), mean(precio_hidrogeno, na.rm=TRUE)) %>% view()

#iv. Imprima en un mapa interactivo, la localización del top 10 mas caras y  otro  mapa  interactivo  del  top  20  mas  baratas,  estos  2  archivos deben guardarse en formato HTML y pdf para su posterior entrega al  inversor.,  nombre  de  los  archivos  :  top_10.html,  top_10.pdf  y top_20.html, top_20.pdf 
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_95_e5, localidad, direccion) %>% 
  top_n(10, precio_gasolina_95_e5) %>%  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_95_e5 )

ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasolina_95_e5, localidad, direccion) %>% 
  top_n(-20, precio_gasolina_95_e5) %>%  leaflet() %>% addTiles() %>%  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasolina_95_e5 )
lowcost_22176323 <- ds
View(lowcost_22176323)

#v.conseguidos  objetivos  anteriores,  debe  guardar  este  "archivo"  en  una nueva tabla llamada low-cost_num_expediente y deberá estar disponible  también  en  su  repositorio  de  Github  con  el  mismo nombre y formato csv. 
write.csv(lowcost_22176323,"C:/Users/hola/Desktop/Spark/lowcost_22176323.csv", row.names = FALSE)

#B.Este empresario tiene sus residencias habituales en Madrid y Barcelona , por  lo  que,  en  principio  le  gustaría  implantarse  en  cualquiera  de  las  dos antes citadas, y para ello quiere saber : 

#i. cuántas gasolineras tiene la comunidad de Madrid y en la comunidad de Cataluña, cuántas son low-cost, cuantas no lo son,  
MyB <- ds %>% select(idccaa, lowcost, provincia) %>% 
  filter(idccaa=='13'|idccaa=='09') %>% 
  group_by(idccaa) %>% count(lowcost) %>% 
  View()

#ii. además,  necesita  saber  cuál  es  el precio  promedio,  el precio  más bajo  y  el  más  caro  de  los  siguientes  carburantes:  gasóleo  A,  y gasolina 95 e Premium.  
informe_MAD_BCN_22176323 <- ds %>% select(idccaa, lowcost, provincia, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% 
  filter(idccaa=="09" | idccaa=='13') %>%  
  drop_na() %>% 
  group_by(idccaa, lowcost) %>% 
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium)) 
View(informe_MAD_BCN_22176323)


#iii. Conseguido el objetivo, deberá guardar este "archivo" en  una nueva tabla llamada informe_MAD_BCN_expediente y deberá estar disponible  también  en  su  repositorio  con  el  mismo  nombre  en formato csv 
write.csv(informe_MAD_BCN_22176323,"C:/Users/hola/Desktop/Spark/informe_MAD_BCN_22176323.csv", row.names = FALSE)

#c. Por  sí  las  comunidades  de  Madrid  y  Cataluña  no  se  adapta  a  sus requerimientos, el empresario también quiere : 

#i. conocer  a  nivel  municipios,  cuántas  gasolineras  son  low-cost, cuantas no lo son, cuál es el precio promedio, el precio más bajo y el más caro de los siguientes carburantes: gasóleo A, y gasolina 95 e5 Premium , en todo el  TERRITORIO NACIONAL, exceptuando las grandes CIUDADES ESPAÑOLAS ("MADRID", "BARCELONA", "SEVILLA" y "VALENCIA") 
informe_no_grandes_ciudades_22176323 <- ds %>% select(idccaa, id_municipio, municipio, lowcost, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% 
  group_by(municipio, lowcost) %>% 
  filter(!municipio %in% c("Madrid", "Barcelona", "Sevilla", "Valencia")) %>%
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium)) 
View(informe_no_grandes_ciudades_22176323)

Cantidad_gasolineras <- ds %>% select(id_municipio, municipio, lowcost) %>% 
  filter(!municipio %in% c("Madrid", "Barcelona", "Sevilla", "Valencia")) %>% 
  group_by(municipio) %>% 
  count(lowcost)
View(Cantidad_gasolineras)

#ii. Conseguido el objetivo, deberá guardar este "archivo" en  una nueva tabla llamada informe_no_grandes_ciudades_expediente y deberá  estar  disponible  también  en  su  repositorio  con  el  mismo nombre en formato Excel 
install.packages("writexl")
library("writexl")

write_xlsx(informe_no_grandes_ciudades_22176323,"C:/Users/hola/Desktop/Spark/informe_no_grandes_ciudades_22176323.xlsx")
write_xlsx(Cantidad_gasolineras,"C:/Users/hola/Desktop/Spark/informe_no_grandes_ciudades_count_22176323.xlsx")
#D. Determine :
#i. que gasolineras se encuentran abiertas las 24 horas exclusivamente, genere una nueva tabla llamada no_24_horas sin la variable horario ( es decir no debe aparecer esta columna). 
no_24_horas <- ds[(ds$horario == 'L-D: 24H'),] 
no_24_horas <- no_24_horas[,-3]
View(no_24_horas)

#ii. Conseguido el objetivo, deberá guardar este "archivo" en  una nueva tabla llamada no_24_horas y deberá estar disponible también en su repositorio con el mismo nombre en formato Excel 
write_xlsx(no_24_horas,"C:/Users/hola/Desktop/Spark/no_24_horas_22176323.xlsx")

#E. Uno de los factores más importantes para que el empresario se decante a instalar nuevas gasolineras es la demanda que viene dada por la población y  la  competencia  existente  en  un  municipio donde  se  pretenda  implantar las gasolineras, para responder a esta pregunta de negocio

#i. deberá  añadir  la  población  al  dataset  original  creando  una  nueva columna denominada población, esta información debe ser veraz y la más actualizada, la población debe estar a nivel municipal ( todo el territorio nacional)
library(readxl)
pobmun21 <- read_excel("pobmun21.xlsx", skip = 2)%>% drop_na() %>% clean_names()

poblacion <- rename(pobmun21, id_provincia = 'cpro', id_municipio='cmun', municipio='nombre')

union <- left_join(ds, poblacion, 'municipio')
View(union)

#He tratado de realizarlo de diversas formas para escribir un join incluyendo all.x = TRUE, pero el resultado incluye un número muy elevado de filas
#A pesar de que este problema, voy a seguir con el resto de enunciados 

#ii. este  empresario  ha  visto  varios  sitios  donde  potencialmente  le gustaría instalar su gasolinera, eso sitios están representados por la dirección,  desde  esta ultima  calcule  cuanta  competencia  (  nombre de la gasolinera y direccion) tiene en : 
#1. En un  radio de 1 km ( genere mapa_competencia1.html) 
#2. En un  radio de 2 km ( genere mapa_competencia2.html) 
#3. En un  radio de 4 km ( genere mapa_competencia3.html)


#iii. genere  el  TopTen  de  municipios  entre  todo  el  territorio  nacional excepto el territorio insular,  donde no existan gasolineras 24 horas, agrupadas entre low-cost y no low-cost, deberá guardar este "archivo"  en    una  nueva  tabla  llamada informe_top_ten_expediente y deberá estar disponible también en su repositorio con el mismo nombre en formato csv. 
informe_top_ten_22176323 <- union %>% 
 filter(!provincia.x%in%c('BALEARS (ILLES)', 'PALMAS (LAS)')) %>% 
  filter(!horario == 'L-D: 24H') %>% 
  group_by(municipio,lowcost) %>% 
  count() 
informe_top_ten_22176323 <- informe_top_ten22176323[order(informe_top_ten22176323$n, decreasing = TRUE),]  
informe_top_ten_22176323 <- head(informe_top_ten22176323,10)
View(informe_top_ten_22176323)
write.csv(informe_top_ten_22176323,"C:/Users/hola/Desktop/Spark/informe_top_ten_22176323.csv", row.names = FALSE)

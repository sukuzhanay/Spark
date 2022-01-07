pacman::p_load(httr, tidyverse, leaflet, janitor, readr)
#Datos
url <- "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
httr::GET(url)
sc<-spark_connect(master="local")

#A)1)Limpiar Dataset
jsonlite::fromJSON(url)
ds<- jsonlite::fromJSON(url) 
ds<-ds$ListaEESSPrecio
ds <- ds %>% as_tibble() %>% clean_names() %>% type_convert(locale = locale(decimal_mark = ",")) %>% view()

#A)2) genere un informe y explique si encuentra alguna anomalía, en el punto ii.
#Se explica en la Memeria descriptiva
#A)3). cree una columna nueva que deberá llamarse low-cost, y determine
#cuál es el precio promedio de todos los combustibles a nivel
#comunidades autónomas, así como para las provincias, tanto para
#el territorio peninsular e insular, esta columna esta columna deberá clasificar las
#estaciones por lowcost y no lowcost
ds_lowcost <- ds %>% mutate(low_cost=!rotulo%in%c('REPSOL', 'BP', 'CEPSA', 'CAMPSA', 'SHELL','GALP')) %>% view()

#MEDIA DE LAS COMUNIDADES
media_CA <- ds_lowcost %>% select(precio_bioetanol, precio_biodiesel, precio_gas_natural_comprimido,precio_gas_natural_licuado, precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, precio_gasolina_98_e5, precio_hidrogeno, rotulo, idccaa, provincia) %>% 
  group_by(idccaa) %>% summarise(media_precio_bioetanol=mean(precio_bioetanol, na.rm=TRUE), mean(precio_biodiesel, na.rm=TRUE), mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), mean(precio_hidrogeno, na.rm=TRUE)) %>% 
  view()
#MEDIA DE LAS PROVINCIAS
media_PRO <- ds_lowcost %>% select(precio_bioetanol, precio_biodiesel, precio_gas_natural_comprimido,precio_gas_natural_licuado, precio_gases_licuados_del_petroleo, precio_gasoleo_a, precio_gasoleo_b, precio_gasoleo_premium, precio_gasolina_95_e10, precio_gasolina_95_e5, precio_gasolina_95_e5_premium, precio_gasolina_98_e10, precio_gasolina_98_e5, precio_hidrogeno, rotulo, idccaa, provincia) %>% 
  group_by(provincia) %>% summarise(media_precio_bioetanol=mean(precio_bioetanol, na.rm=TRUE), mean(precio_biodiesel, na.rm=TRUE), mean(precio_gas_natural_comprimido, na.rm=TRUE), mean(precio_gas_natural_licuado, na.rm=TRUE), mean(precio_gases_licuados_del_petroleo, na.rm=TRUE), mean(precio_gasoleo_a, na.rm=TRUE), mean(precio_gasoleo_b, na.rm=TRUE), mean(precio_gasoleo_premium, na.rm=TRUE), mean(precio_gasolina_95_e5, na.rm=TRUE), mean(precio_gasolina_95_e5_premium, na.rm=TRUE), mean(precio_gasolina_98_e10, na.rm=TRUE), mean(precio_gasolina_98_e5, na.rm=TRUE), mean(precio_hidrogeno, na.rm=TRUE)) %>%
  view()

#A)4)Imprima en un mapa interactivo, la localización del top 10 mas caras
#y otro mapa interactivo del top 20 mas baratas, estos 2 archivos
#deben guardarse en formato HTML y pdf
#TOP 10 CARAS
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasoleo_b, localidad, direccion) %>% 
  top_n(10, precio_gasoleo_b) %>%  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasoleo_b)

#TOP 20 BARATAS
ds %>%  select(rotulo, latitud, longitud_wgs84, precio_gasoleo_b, localidad, direccion) %>% 
  top_n(-20, precio_gasoleo_b) %>%  leaflet() %>% addTiles() %>%
  addCircleMarkers(lng = ~longitud_wgs84, lat = ~latitud, popup = ~rotulo,label = ~precio_gasoleo_b)

#A)5) csv
setwd("C:/Users/rodri/OneDrive/Escritorio/masteeeer/TOP")
write.table(ds_lowcost, file = "low_cost_21440124.csv", sep = ";", row.names = F)

#B)Este empresario tiene sus residencias habituales en Madrid y Barcelona ,
#por lo que, en principio le gustaría implantarse en cualquiera de las dos
#antes citadas, y para ello quiere saber :
#B)1). cuántas gasolineras tiene la comunidad de Madrid y en la
#comunidad de Cataluña, cuántas son low-cost, cuantas no lo son,

Madrid<-ds_lowcost %>% select(idccaa, low_cost, provincia) %>%
  filter(idccaa=="13") %>% count(low_cost) %>% View()
Cataluna<-ds_lowcost %>% select(idccaa, low_cost, provincia) %>% 
  filter(idccaa=="09") %>% count(low_cost) %>% View

#B)2)además, necesita saber cuál es el precio promedio, el precio más
#bajo y el más caro de los siguientes carburantes: gasóleo A, y
#gasolina 95 e Premium.

Madrid_gaso <- ds_lowcost %>% select(idccaa, low_cost, provincia, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>%
  filter(idccaa=="13") %>%  drop_na() %>% 
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium)) 
  View(Madrid_gaso)

Cataluna_gaso <- ds_lowcost %>% select(idccaa, low_cost, provincia, precio_gasoleo_a, precio_gasolina_95_e5_premium) %>% 
  filter(idccaa=="09") %>%  drop_na() %>% 
  summarise(max(precio_gasoleo_a), min(precio_gasoleo_a), mean(precio_gasoleo_a), max(precio_gasolina_95_e5_premium), min(precio_gasolina_95_e5_premium), mean(precio_gasolina_95_e5_premium)) 
  View(Cataluna_gaso)
  
gaso_1M_2B <- merge(x = Madrid_gaso, y = Cataluna_gaso, all = TRUE) 
view(gaso_1M_2B)

#B)3) CSV

write.table(gaso_1M_2B, file = "informe_MAD_BCN_21440124.csv", sep = ";", row.names = F)

#C)Por sí las comunidades de Madrid y Cataluña no se adapta a sus
#requerimientos, el empresario también quiere :
#C)1)conocer a nivel municipios, cuántas gasolineras son low-cost,
#cuantas no lo son, cuál es el precio promedio, el precio más bajo y
#el más caro de los siguientes carburantes: gasóleo A, y gasolina 95
#e5 Premium , en todo el TERRITORIO NACIONAL, exceptuando las
#grandes CIUDADES ESPAÑOLAS ("MADRID", "BARCELONA", "SEVILLA" y "VALENCIA")

no_grandes <- ds_lowcost %>% select(rotulo, idccaa, low_cost, precio_gasoleo_a, precio_gasolina_95_e5_premium, municipio, id_municipio) %>% 
  group_by(municipio, low_cost) %>% 
  filter(!municipio %in% c("Madrid", "Barcelona", "Sevilla", "Valencia")) %>% summarise(rotulo, mean(precio_gasoleo_a, na.rm = TRUE), mean(precio_gasolina_95_e5_premium, na.rm = TRUE), max(precio_gasoleo_a, na.rm = TRUE), max(precio_gasolina_95_e5_premium, na.rm = TRUE), min(precio_gasoleo_a, na.rm = TRUE), min(precio_gasolina_95_e5_premium, na.rm = TRUE))
  
View(no_grandes)

low_o_no <- no_grandes %>% group_by(low_cost) %>% count(low_cost)
View(low_o_no)

#C)2) CSV
write.table(no_grandes, file = "informe_no_grandes_ciudades_21440124.csv", sep = ";", row.names = F)


#D)1)Considere que gasolineras se encuentran abiertas las 24 horas
#exclusivamente, genere una nueva tabla llamada no_24_horas sin
#la variable horario ( es decir no debe aparecer esta columna).
no_24_horas <- ds %>% select(rotulo, direccion,localidad,municipio,id_provincia,ideess) %>% 
  filter(ds$horario=="L-D: 24H")
View(no_24_horas)

#D)2) CSV
write.table(no_24_horas, file = "no_24_horas.csv", sep = ";", row.names = F)


from datetime import datetime
from elasticsearch import Elasticsearch
class ConsultaElastic:

    def __init__(self):
        self.index = ".ds-logs-nginx.access-default-2022.11.11-000001"
        self.index2 = ".ds-logs-nginx.access-default-2022.11.25-000001"
        self.index_bot = "boot_google"
        self.id = "electivo_stack_elk:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRiZjIxYTgwYjdlZTg0NjFkOGM4ODRjYTQ0N2MxZjk2MSQwZmM2YzdkMjk2MDQ0OWM3ODI4YjMxMTc0NDI0ZjA0OQ=="
        self.id2 = "proyect_elk_maquina_2:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDQzY2U2MjgyZmJlMDQ0YmM5Y2U0N2Y2MzhiNzkyNjJhJGFkMDFlYWI5YTNhNTQ4OWU5NTI3ZmYyYTU1OGRjODE2"
        self.auth = ("elastic", "IdNxS6CkwiOt8646roR0NALW")
        self.auth2 = ("elastic","GGjZe1a9szDfA3747SLD9NiQ")
        self.fecha_Actual = datetime.now()
        self.es = Elasticsearch(cloud_id = self.id2, basic_auth= self.auth2)

    def rastrear_ip_bot(self):
        consulta_get_all ={
             "match_all": {}
        }
        consulta ={
             "bool": {
                "must_not":[
                    {"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}
                    ],
                "filter": [
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    ]
            }
        }
        ip_bot_que_entraron= []
        flag = False
        result_ip_bot = self.es.search(index= self.index_bot, query=consulta_get_all, size=10000)
        result_log_nginx = self.es.search(index= self.index2, query=consulta, size=10000)
        for doc in result_log_nginx['hits']['hits']:
            ip_remote = doc['_source']['source']['ip']
            # print("ip remote : "+ip_remote)
            # print(ip_remote)
            for doc2 in result_ip_bot['hits']['hits']:
                ip_bot = doc2['_source']['ipv4Prefix']
                # print("ip bot: "+ip_bot)
                # print(ip_bot)
                if ip_remote == ip_bot:
                    flag = True
                    ip_bot_que_entraron.append(ip_remote)
                    print("IP BOT: ", ip_bot)
                    print("IP NGINX: ", ip_remote)
                    # print("URL: ", doc2['_source']['url'])
                    # print("USER AGENT: ", doc2['_source']['user_agent'])
                    # print("FECHA: ", doc2['_source']['timestamp'])
                    print("---------------------------------------------------------------")
        if flag == False:
            print("por el momento no ha sido ratreado por los bot de google")
            print("---------------------------------------------------------------")
            return "sin movimimentos"
        else:
            print("ya ha sido ratreado por los bot de google")
            return ip_bot_que_entraron

    def obtener_cantidad_de_visitas_mes_actual(self):
        #fecha_Actual = datetime.now()
        # print(fecha_Actual.year)
        # print(fecha_Actual.month)

        consulta = {
            "bool": {
                "must_not":[
                    {"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}
                    ],
                "filter": [
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    ]
            }
        }

        result = self.es.search(index= self.index2, query=consulta, size=10000)
        # print(type(result))
        print("obtener_cantidad_de_visitas_mes_actual")
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()

    def obtener_cantidad_visitas_desde_chile_mes_Actual(self):
        # fecha_Actual = datetime.now()
        consulta = {
            "bool": {
                "must": [{"match": {"source.geo.country_name": "Chile"}}],
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # { "range": { "@timestamp": { "gte": "2022-11-01T02:00:00.000Z" } } }
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }

                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        # print(type(result))
        print("obtener_cantidad_visitas_desde_chile_mes_Actual")
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()

    def obtener_cantidad_visitas_desde_extranjero_mes_actual(self):
        # fecha_Actual = datetime.now()
        consulta = {
            "bool": {
                "must_not":[
                    {"match": {"nginx.access.remote_ip_list": "127.0.0.1"}},
                    {"match": {"source.geo.country_name": "Chile"}}
                    ],
                "filter": [
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        # print(type(result))
        print("obtener_cantidad_visitas_desde_extranjero_mes_actual")
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()

    def obtener_cantidad_visitas_por_mes_desde_chile(self):
        # fecha_Actual = datetime.now()
        visitas_por_mes = []
        consulta = {
            "bool": {
                "must":[
                        {"match": {"source.geo.country_name": "Chile"}}
                    ],
                "must_not":[
                    {"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}
                    ],
                "filter":[
                    # {"match": {"source.geo.country_iso_code": "CL"}},
                    # {"match": {"source.geo.source.geo.country_name": "Chile"}},
                    {"range": {"@timestamp": { "gte": str(self.fecha_Actual.year)+"-01-01T00:00:00.000Z"}}}
                    ]
                    # "lte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+str(self.fecha_Actual.day)+"T0"+str(self.fecha_Actual.hour)+":"+str(self.fecha_Actual.minute)+":"+str(self.fecha_Actual.second)+".000Z"} } 
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        ener,feb,mar,abril,mayo,junio,julio,agos,sept,oct,nov,dic= 0,0,0,0,0,0,0,0,0,0,0,0
        for doc in result['hits']['hits']:
            # print(doc)
            fecha = doc['_source']['@timestamp']
            # print(fecha)
            mes = fecha[5:7]
            # print(mes)
            if mes == "01":
                ener += 1
            elif mes == "02":
                feb += 1
            elif mes == "03":
                mar += 1
            elif mes == "04":
                abril += 1
            elif mes == "05":
                mayo += 1
            elif mes == "06":
                junio += 1
            elif mes == "07":
                julio += 1
            elif mes == "08":
                agos += 1
            elif mes == "09":
                sept += 1
            elif mes == "10":
                oct += 1
            elif mes == "11":
                nov += 1
            elif mes == "12":
                dic += 1
        visitas_por_mes.append(ener)
        visitas_por_mes.append(feb)
        visitas_por_mes.append(mar)
        visitas_por_mes.append(abril)
        visitas_por_mes.append(mayo)
        visitas_por_mes.append(junio)
        visitas_por_mes.append(julio)
        visitas_por_mes.append(agos)
        visitas_por_mes.append(sept)
        visitas_por_mes.append(oct)
        visitas_por_mes.append(nov)
        visitas_por_mes.append(dic)
        print("obtener_cantidad_visitas_por_mes_desde_chile")
        print(visitas_por_mes)
        print(type(visitas_por_mes))
        return visitas_por_mes
    
    def principal_proveedor_del_mes_en_chile(self):
         # fecha_Actual = datetime.now()
        claro={"nombre":"Claro Chile S.A.","cantidad":0}
        entel={"nombre":"Entel PCS S.A.","cantidad":0}
        vtr={"nombre":"VTR Banda Ancha S.A.","cantidad":0}
        telefonica={"nombre":"TELEFONICA CHILE S.A.","cantidad":0}
        telmex={"nombre":"Telmex Servicios Empresariales S.A.","cantidad":0}
        aux=[claro,entel,vtr,telefonica,telmex]
        consulta = {
            "bool": {
                "must": [{"match": {"source.geo.country_name": "Chile"}}],
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }

                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        
        proveedores_en_chile = []
        for doc in result['hits']['hits']:
            proveedor = doc['_source']['source']['as']['organization']['name']
            proveedores_en_chile.append(proveedor)
        for proveedor in proveedores_en_chile:
            if proveedor == "Claro Chile S.A.":
                claro['cantidad'] += 1
            elif proveedor == "Entel PCS S.A.":
                entel['cantidad'] += 1
            elif proveedor == "VTR Banda Ancha S.A.":
                vtr['cantidad'] += 1
            elif proveedor == "TELEFONICA CHILE S.A.":
                telefonica['cantidad'] += 1
            elif proveedor == "Telmex Servicios Empresariales S.A.":
                telmex['cantidad'] += 1 
        print("principal_proveedor_del_mes_en_chile")
        # print(aux)
        max_value = None
        dic_aux = None
        for dic in aux:
            if max_value is None or dic['cantidad'] > max_value:
                max_value = dic['cantidad']
                dic_aux = dic
        print(dic_aux)
        return dic_aux['nombre']
    
    def obtener_visitas_desde_EE_UU(self):
        consulta = {
            "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } },
                    {"match": {"source.geo.country_name": "United States"}}
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        return result['hits']['hits'].__len__()
    
    def obtener_visitas_desde_canada(self):
        consulta = {
            "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}},
                            
                    ],
                "filter": [
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } },
                    {"match": {"source.geo.country_name": "Canada"}}
                    ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        return result['hits']['hits'].__len__()
    
    def obtener_visitas_desde_brazil(self):
        consulta = {
            "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } },
                    {"match": {"source.geo.country_name": "Brazil"}}
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        return result['hits']['hits'].__len__()
    
    def obtener_visitas_desde_alemania(self):
        consulta = {
            "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } },
                    {"match": {"source.geo.country_name": "Alemania"}}
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        return result['hits']['hits'].__len__()
    
    def obtener_visitas_desde_españa(self):
        consulta = {
            "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } },
                    {"match": {"source.geo.country_name": "España"}}
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        return result['hits']['hits'].__len__()
    
    def consultar_trafico_por_hora_9_10(self):
        # consulta por la cantidad de trafico por hora entre las 9 y las 10 de la mañana
        fecha_Actual = str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+str(self.fecha_Actual.day)+"T"
        consulta ={
             "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    # { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    { "range": { "@timestamp": { "gte": fecha_Actual+"09:00:00.000Z","lte":fecha_Actual+"10:00:00.000Z" } } }
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()
    
    def consultar_trafico_por_hora_10_11(self):
        # consulta por la cantidad de trafico por hora entre las 9 y las 10 de la mañana
        fecha_Actual = str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+str(self.fecha_Actual.day)+"T"
        consulta ={
             "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    # { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    { "range": { "@timestamp": { "gte": fecha_Actual+"10:00:01.000Z","lte":fecha_Actual+"11:00:00.000Z" } } }
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()
    
    def consultar_trafico_por_hora_11_12(self):
        # consulta por la cantidad de trafico por hora entre las 9 y las 10 de la mañana
        fecha_Actual = str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+str(self.fecha_Actual.day)+"T"
        consulta ={
             "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    # { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    { "range": { "@timestamp": { "gte": fecha_Actual+"11:00:01.000Z","lte":fecha_Actual+"12:00:00.000Z" } } }
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()
    
    def consultar_trafico_por_hora_12_13(self):
        # consulta por la cantidad de trafico por hora entre las 9 y las 10 de la mañana
        fecha_Actual = str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+str(self.fecha_Actual.day)+"T"
        consulta ={
             "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    # { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    { "range": { "@timestamp": { "gte": fecha_Actual+"12:00:01.000Z","lte":fecha_Actual+"13:00:00.000Z" } } }
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()
    
    def consultar_trafico_por_hora_13_14(self):
        # consulta por la cantidad de trafico por hora entre las 9 y las 10 de la mañana
        fecha_Actual = str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+str(self.fecha_Actual.day)+"T"
        consulta ={
             "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    # { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    { "range": { "@timestamp": { "gte": fecha_Actual+"13:00:01.000Z","lte":fecha_Actual+"14:00:00.000Z" } } }
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()
    
    def consultar_trafico_por_hora_14_15(self):
        # consulta por la cantidad de trafico por hora entre las 9 y las 10 de la mañana
        fecha_Actual = str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+str(self.fecha_Actual.day)+"T"
        consulta ={
             "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    # { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    { "range": { "@timestamp": { "gte": fecha_Actual+"14:00:01.000Z","lte":fecha_Actual+"15:00:00.000Z" } } }
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()
    
    def consultar_trafico_por_hora_15_16(self):
        # consulta por la cantidad de trafico por hora entre las 9 y las 10 de la mañana
        fecha_Actual = str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+str(self.fecha_Actual.day)+"T"
        consulta ={
             "bool": {
                "must_not":[{"match": {"nginx.access.remote_ip_list": "127.0.0.1"}}],
                "filter": [
                    # formato de fecha del log "2022-11-01T02:00:00.000Z"
                    # { "range": { "@timestamp": { "gte": str(self.fecha_Actual.year)+"-"+str(self.fecha_Actual.month)+"-"+"01T00:00:00.000Z" } } }
                    { "range": { "@timestamp": { "gte": fecha_Actual+"17:00:01.000Z","lte":fecha_Actual+"19:00:00.000Z" } } }
                ]
            }
        }
        result = self.es.search(index= self.index2, query=consulta, size=10000)
        print(result['hits']['hits'].__len__())
        return result['hits']['hits'].__len__()
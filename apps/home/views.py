# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django import template
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader
from django.urls import reverse
from apps.home.ConsultaElastic import ConsultaElastic
from datetime import datetime


@login_required(login_url="/login/")
def index(request):
    date = datetime.now()
    fecha = date.strftime("%d/%m/%Y")
    consulta = ConsultaElastic()   
    total_visitas_mes_actual = consulta.obtener_cantidad_de_visitas_mes_actual()
    cantidad_visitas_desde_chile_mes_Actual = consulta.obtener_cantidad_visitas_desde_chile_mes_Actual()
    cantidad_visitas_desde_extranjero_mes_actual = consulta.obtener_cantidad_visitas_desde_extranjero_mes_actual()
    cantidad_visitas_por_mes_desde_chile = consulta.obtener_cantidad_visitas_por_mes_desde_chile()
    principal_proveedor_chile_mes_actual=consulta.principal_proveedor_del_mes_en_chile()
    visitas_usa=consulta.obtener_visitas_desde_EE_UU()
    visitas_canada=consulta.obtener_visitas_desde_canada()
    visitas_brazil=consulta.obtener_visitas_desde_brazil()
    visitas_alemania = consulta.obtener_visitas_desde_alemania()
    visitas_españa = consulta.obtener_visitas_desde_españa()
    trafico_9_10 = consulta.consultar_trafico_por_hora_9_10()
    trafico_10_11 = consulta.consultar_trafico_por_hora_10_11()
    trafico_11_12 = consulta.consultar_trafico_por_hora_11_12()
    trafico_12_13 = consulta.consultar_trafico_por_hora_12_13()
    trafico_13_14 = consulta.consultar_trafico_por_hora_13_14()
    trafico_14_15 = consulta.consultar_trafico_por_hora_14_15()
    trafico_15_16 = consulta.consultar_trafico_por_hora_15_16()
    rastreo = consulta.rastrear_ip_bot()
    context = {'segment': 'index',
               'total_visitas_mes_actual': total_visitas_mes_actual,
               'cantidad_visitas_desde_chile_mes_Actual': cantidad_visitas_desde_chile_mes_Actual,
               'cantidad_visitas_desde_extranjero_mes_actual': cantidad_visitas_desde_extranjero_mes_actual,
               'cantidad_visitas_por_mes_desde_chile': cantidad_visitas_por_mes_desde_chile,
               'principal_proveedor_chile_mes_actual': principal_proveedor_chile_mes_actual,
               'visitas_usa': visitas_usa,
               'visitas_canada': visitas_canada,
               'visitas_brazil': visitas_brazil,
               'visitas_alemania': visitas_alemania,
               'visitas_españa': visitas_españa,
               'trafico_9_10': trafico_9_10,
                'fecha': fecha,
                'trafico_10_11': trafico_10_11,
                'trafico_11_12': trafico_11_12,
                'trafico_12_13': trafico_12_13,
                'trafico_13_14': trafico_13_14,
                'trafico_14_15': trafico_14_15,
                'trafico_15_16': trafico_15_16,
                'rastreo': rastreo
                }
    html_template = loader.get_template('home/index.html')
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def pages(request):
    context = {}
    # All resource paths end in .html.
    # Pick out the html file name from the url. And load that template.
    try:

        load_template = request.path.split('/')[-1]

        if load_template == 'admin':
            return HttpResponseRedirect(reverse('admin:index'))
        context['segment'] = load_template

        html_template = loader.get_template('home/' + load_template)
        return HttpResponse(html_template.render(context, request))

    except template.TemplateDoesNotExist:

        html_template = loader.get_template('home/page-404.html')
        return HttpResponse(html_template.render(context, request))

    except:
        html_template = loader.get_template('home/page-500.html')
        return HttpResponse(html_template.render(context, request))
